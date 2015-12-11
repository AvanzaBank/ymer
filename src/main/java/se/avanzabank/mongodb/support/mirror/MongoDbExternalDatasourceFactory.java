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
package se.avanzabank.mongodb.support.mirror;

import javax.annotation.PreDestroy;

import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.mongodb.ReadPreference;

import se.avanzabank.mongodb.util.LifecycleAware;
import se.avanzabank.mongodb.util.LifecycleContainer;
import se.avanzabank.mongodb.util.Require;
/**
 * Factory for building a ManagedDataSourceAndBulkDataPersister. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 */
public final class MongoDbExternalDatasourceFactory implements LifecycleAware {
	
	private final MongoDbFactory mongoDbFactory;
	private final MongoConverter mongoConverter;
	private final MirroredDocuments mirroredDocuments;
	private MirrorExceptionListener exceptionListener = new MirrorExceptionListener() {
		@Override
		public void onMirrorException(Exception e, MirrorOperation failedOperation, Object[] failedObjects) {}
	};
	private final LifecycleContainer lifecycleContainer = new LifecycleContainer();
	private ReadPreference readPreference = ReadPreference.primary();

	public MongoDbExternalDatasourceFactory(MirroredDocuments mirroredDocuments, MongoDbFactory mongoDbFactory, MongoConverter mongoConverter) {
		this.mirroredDocuments = Require.notNull(mirroredDocuments);
		this.mongoDbFactory = Require.notNull(mongoDbFactory);
		this.mongoConverter = Require.notNull(mongoConverter);
		// Set the event publisher to null to avoid deadlocks when loading data in parallel
		if (mongoConverter.getMappingContext() instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware)mongoConverter.getMappingContext()).setApplicationEventPublisher(null);
		}
	}

	public ManagedDataSourceAndBulkDataPersister create() {
		DocumentDb mongoDb = DocumentDb.mongoDb(mongoDbFactory.getDb(), readPreference);
		SpaceMirrorContext mirrorContext = new SpaceMirrorContext(mirroredDocuments,
				DocumentConverter.mongoConverter(mongoConverter), mongoDb, exceptionListener);
		VersionedMongoDBExternalDataSource versionedMongoDBExternalDataSource = new VersionedMongoDBExternalDataSource(mirrorContext);
		lifecycleContainer.add(versionedMongoDBExternalDataSource);
		return versionedMongoDBExternalDataSource;
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

}
