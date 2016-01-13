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
package com.avanza.ymer.mirror;

import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.DB;
import com.mongodb.ReadPreference;
/**
 * @author Elias Lindholm (elilin)
 *
 */
public final class MongoDbExternalDatasourceFactory {
	
	private MirrorExceptionListener exceptionListener = new MirrorExceptionListener() {
		@Override
		public void onMirrorException(Exception e, MirrorOperation failedOperation, Object[] failedObjects) {}
	};
	private ReadPreference readPreference = ReadPreference.primary();
	private SpaceMirrorContext mirrorContext;
	private boolean exportExceptionHandleMBean = true;

	public MongoDbExternalDatasourceFactory(MirroredDocuments mirroredDocuments, DB db, MongoConverter mongoConverter) {
		DocumentDb documentDb = DocumentDb.mongoDb(db, readPreference);
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(mongoConverter);
		mirrorContext = new SpaceMirrorContext(mirroredDocuments, documentConverter, documentDb, exceptionListener);
		// Set the event publisher to null to avoid deadlocks when loading data in parallel
		if (mongoConverter.getMappingContext() instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware)mongoConverter.getMappingContext()).setApplicationEventPublisher(null);
		}
	}

	public SpaceDataSource createSpaceDataSource() {
		return new MongoSpaceDataSource(mirrorContext);
	}
	
	public void setExportExceptionHandlerMBean(boolean exportExceptionHandleMBean) {
		this.exportExceptionHandleMBean = exportExceptionHandleMBean;
	}

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		MongoSpaceSynchronizationEndpoint mongoSpaceSynchronizationEndpoint = new MongoSpaceSynchronizationEndpoint(mirrorContext);
		if (this.exportExceptionHandleMBean) {
			mongoSpaceSynchronizationEndpoint.registerExceptionHandlerMBean();
		}
		return mongoSpaceSynchronizationEndpoint;
	}
	
	/**
	 * Sets a MirrorExceptionListener (optional). <p>
	 * 
	 * @param exceptionListener
	 */
	public void setExceptionListener(MirrorExceptionListener exceptionListener) {
		this.exceptionListener = exceptionListener;
	}

}
