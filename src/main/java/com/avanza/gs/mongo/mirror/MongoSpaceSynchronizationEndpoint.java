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
package com.avanza.gs.mongo.mirror;

import javax.annotation.PostConstruct;

import com.avanza.gs.mongo.CatchesAllDocumentWriteExceptionHandler;
import com.avanza.gs.mongo.RethrowsTransientDocumentWriteExceptionHandler;
import com.avanza.gs.mongo.ToggleableDocumentWriteExceptionHandler;
import com.avanza.gs.mongo.mbean.MBeanRegistrationUtil;
import com.avanza.gs.mongo.mbean.MBeanRegistrator;
import com.avanza.gs.mongo.mbean.PlatformMBeanServerMBeanRegistrator;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

final class MongoSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {
	
	private final MirroredDocumentWriter mirroredDocumentWriter;
	private final ToggleableDocumentWriteExceptionHandler exceptionHandler;
	private final MBeanRegistrator mbeanRegistrator;

	public MongoSpaceSynchronizationEndpoint(SpaceMirrorContext spaceMirror) {
		exceptionHandler = ToggleableDocumentWriteExceptionHandler.create(
				new RethrowsTransientDocumentWriteExceptionHandler(),
				new CatchesAllDocumentWriteExceptionHandler());
		this.mirroredDocumentWriter = new MirroredDocumentWriter(spaceMirror, exceptionHandler);
		this.mbeanRegistrator = new PlatformMBeanServerMBeanRegistrator();
	}

	@Override
	public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
		mirroredDocumentWriter.executeBulk(batchData);
	}
	
	@PostConstruct
	public void registerExceptionHandlerMBean() {
		MBeanRegistrationUtil.registerExceptionHandlerMBean(mbeanRegistrator, exceptionHandler);
	}
	
}
