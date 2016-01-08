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

import javax.annotation.PreDestroy;

import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.avanza.gs.mongo.util.LifecycleAware;
import com.avanza.gs.mongo.util.LifecycleContainer;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.DB;
import com.mongodb.ReadPreference;
/**
 * @author Elias Lindholm (elilin)
 *
 */
public final class MongoDbExternalDatasourceFactory implements LifecycleAware {
	
	private MirrorExceptionListener exceptionListener = new MirrorExceptionListener() {
		@Override
		public void onMirrorException(Exception e, MirrorOperation failedOperation, Object[] failedObjects) {}
	};
	private final LifecycleContainer lifecycleContainer = new LifecycleContainer();
	private ReadPreference readPreference = ReadPreference.primary();
	private VersionedMongoDBExternalDataSource versionedMongoDBExternalDataSource;
	private SpaceMirrorContext mirrorContext;

	public MongoDbExternalDatasourceFactory(MirroredDocuments mirroredDocuments, DB db, MongoConverter mongoConverter) {
		DocumentDb mongoDb = DocumentDb.mongoDb(db, readPreference);
		mirrorContext = new SpaceMirrorContext(mirroredDocuments,
				DocumentConverter.mongoConverter(mongoConverter), mongoDb, exceptionListener);
		versionedMongoDBExternalDataSource = new VersionedMongoDBExternalDataSource(mirrorContext);
//		lifecycleContainer.add(versionedMongoDBExternalDataSource);
		// Set the event publisher to null to avoid deadlocks when loading data in parallel
		if (mongoConverter.getMappingContext() instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware)mongoConverter.getMappingContext()).setApplicationEventPublisher(null);
		}
	}

	public SpaceDataSource createSpaceDataSource() {
		return new VersionedMongoSpaceDataSource(mirrorContext);
	}
	

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		return new VersionedMongoSpaceSynchronizationEndpoint(mirrorContext);
	}
	
	/**
	 * Sets an MirrorExceptionListener (optional). <p>
	 * 
	 * @param exceptionListener
	 */
	public void setExceptionListener(MirrorExceptionListener exceptionListener) {
		this.exceptionListener = exceptionListener;
	}

	@PreDestroy
	public void destroy() {
		lifecycleContainer.destroyAll();
	}
	
	public void registerExceptionHandlerMBean() {
		this.versionedMongoDBExternalDataSource.registerExceptionHandlerMBean();
	}


}
