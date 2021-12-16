/*
 * Copyright 2015 Avanza Bank AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.avanza.ymer;

import static java.util.stream.Collectors.toList;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

final class YmerSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint implements ApplicationContextAware,
		ApplicationListener<ContextRefreshedEvent>, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(YmerSpaceSynchronizationEndpoint.class);

	private final MirroredObjectWriter mirroredObjectWriter;
	private final ToggleableDocumentWriteExceptionHandler exceptionHandler;
	private final PersistedInstanceIdRecalculationService persistedInstanceIdRecalculationService;
	private final SpaceMirrorContext spaceMirror;
	private final ScheduledExecutorService scheduledExecutorService;
	private final Set<ObjectName> registeredMbeans = new HashSet<>();

	private ApplicationContext applicationContext;

	public YmerSpaceSynchronizationEndpoint(SpaceMirrorContext spaceMirror) {
		exceptionHandler = ToggleableDocumentWriteExceptionHandler.create(
				new RethrowsTransientDocumentWriteExceptionHandler(),
				new CatchesAllDocumentWriteExceptionHandler());
		this.spaceMirror = spaceMirror;
		this.mirroredObjectWriter = new MirroredObjectWriter(spaceMirror, exceptionHandler);
		this.persistedInstanceIdRecalculationService = new PersistedInstanceIdRecalculationService(spaceMirror);
		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
	}

	@Override
	public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
		mirroredObjectWriter.executeBulk(batchData);
	}

	@Override
	public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		persistedInstanceIdRecalculationService.setApplicationContext(applicationContext);
	}

	@Override
	public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
		if (event.getApplicationContext().equals(applicationContext)) {
			schedulePersistedIdRecalculationIfNecessary();
		}
	}

	private void schedulePersistedIdRecalculationIfNecessary() {
		List<MirroredObject<?>> objectsToRecalculate = spaceMirror.getMirroredDocuments().stream()
				.filter(MirroredObject::persistInstanceId)
				.filter(MirroredObject::recalculateInstanceIdOnStartup)
				.filter(mirroredObject -> persistedInstanceIdRecalculationService.collectionNeedsCalculation(mirroredObject.getCollectionName()))
				.collect(toList());

		for (MirroredObject<?> mirroredObject : objectsToRecalculate) {
			log.info("Will trigger recalculation of persisted instance id for collections [{}] starting in {} seconds",
					objectsToRecalculate.stream().map(MirroredObject::getCollectionName).collect(Collectors.joining(",")),
					mirroredObject.recalculateInstanceIdWithDelay()
			);
			Runnable task = () -> persistedInstanceIdRecalculationService.recalculatePersistedInstanceId(mirroredObject.getCollectionName());
			scheduledExecutorService.schedule(task, mirroredObject.recalculateInstanceIdWithDelay(), TimeUnit.SECONDS);
		}
	}

	public PersistedInstanceIdRecalculationService getPersistedInstanceIdRecalculationService() {
		return persistedInstanceIdRecalculationService;
	}

	void registerExceptionHandlerMBean() {
		String name = "se.avanzabank.space.mirror:type=DocumentWriteExceptionHandler,name=documentWriteExceptionHandler";
		registerMbean(exceptionHandler, name);
	}

	void registerPersistedInstanceIdRecalculationServiceMBean() {
		String name = "se.avanzabank.space.mirror:type=PersistedInstanceIdRecalculationService,name=persistedInstanceIdRecalculationService";
		registerMbean(persistedInstanceIdRecalculationService, name);
	}

	private void registerMbean(Object object, String name) {
		log.info("Registering MBean with name {}", name);
		try {
			ObjectName objectName = ObjectName.getInstance(name);
			ManagementFactory.getPlatformMBeanServer().registerMBean(object, objectName);
			registeredMbeans.add(objectName);
		} catch (Exception e) {
			log.warn("Failed to register MBean with objectName='{}'", name, e);
		}
	}

	@Override
	public void close() {
		scheduledExecutorService.shutdownNow();
		for (ObjectName registeredMbean : registeredMbeans) {
			try {
				ManagementFactory.getPlatformMBeanServer().unregisterMBean(registeredMbean);
			} catch (Exception e) {
				log.warn("Failed to unregister MBean with objectName='{}'", registeredMbean, e);
			}
		}
	}
}
