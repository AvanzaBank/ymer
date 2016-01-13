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

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

final class MongoSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {
	
	private static final Logger log = LoggerFactory.getLogger(MongoSpaceSynchronizationEndpoint.class);
	
	private final MirroredDocumentWriter mirroredDocumentWriter;
	private final ToggleableDocumentWriteExceptionHandler exceptionHandler;

	public MongoSpaceSynchronizationEndpoint(SpaceMirrorContext spaceMirror) {
		exceptionHandler = ToggleableDocumentWriteExceptionHandler.create(
				new RethrowsTransientDocumentWriteExceptionHandler(),
				new CatchesAllDocumentWriteExceptionHandler());
		this.mirroredDocumentWriter = new MirroredDocumentWriter(spaceMirror, exceptionHandler);
	}

	@Override
	public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
		mirroredDocumentWriter.executeBulk(batchData);
	}
	
	void registerExceptionHandlerMBean() {
		try {
			String name = "se.avanzabank.space.mirror:type=DocumentWriteExceptionHandler,name=documentWriteExceptionHandler";
			log.info("Registering mbean with name {}", name);
			ManagementFactory.getPlatformMBeanServer().registerMBean(exceptionHandler, ObjectName.getInstance(name));
		} catch (Exception e) {
			log.warn("Exception handler MBean registration failed", e);
		}
	}

}
