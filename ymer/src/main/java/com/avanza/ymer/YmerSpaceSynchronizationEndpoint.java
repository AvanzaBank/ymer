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

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

final class YmerSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(YmerSpaceSynchronizationEndpoint.class);

	private final MirroredObjectWriter mirroredObjectWriter;
	private final ToggleableDocumentWriteExceptionHandler exceptionHandler;
	private final Set<ObjectName> registeredMbeans = new HashSet<>();
	private final PerformedOperationMetrics operationStatistics;

	public YmerSpaceSynchronizationEndpoint(SpaceMirrorContext spaceMirror) {
		exceptionHandler = ToggleableDocumentWriteExceptionHandler.create(
				new RethrowsTransientDocumentWriteExceptionHandler(),
				new CatchesAllDocumentWriteExceptionHandler());
		this.operationStatistics = new PerformedOperationMetrics();
		this.mirroredObjectWriter = new MirroredObjectWriter(spaceMirror, exceptionHandler, operationStatistics);
	}

	@Override
	public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
		mirroredObjectWriter.executeBulk(batchData);
	}

	void registerExceptionHandlerMBean() {
		String name = "se.avanzabank.space.mirror:type=DocumentWriteExceptionHandler,name=documentWriteExceptionHandler";
		registerMbean(name, exceptionHandler);
	}

	void registerOperationStatisticsMBean() {
		String name = "se.avanzabank.space.mirror:type=OperationStatistics,name=operationStatistics";
		registerMbean(name, operationStatistics);
	}

	private void registerMbean(String name, Object bean) {
		try {
			log.info("Registering mbean with name {}", name);
			final ObjectName objectName = ObjectName.getInstance(name);
			ManagementFactory.getPlatformMBeanServer().registerMBean(bean, objectName);
			registeredMbeans.add(objectName);
		} catch (Exception e) {
			log.warn("Failed to register MBean with objectName='{}'", name, e);
		}
	}

	@Override
	public void close() {
		for (ObjectName registeredMbean : registeredMbeans) {
			try {
				ManagementFactory.getPlatformMBeanServer().unregisterMBean(registeredMbean);
			} catch (Exception e) {
				log.warn("Failed to unregister MBean with objectName='{}'", registeredMbean, e);
			}
		}
	}
}
