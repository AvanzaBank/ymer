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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
/**
 * @author Elias Lindholm (elilin)
 *
 */
public final class YmerFactory {
	
	private MirrorExceptionListener exceptionListener = new MirrorExceptionListener() {
		@Override
		public void onMirrorException(Exception e, MirrorOperation failedOperation, Object[] failedObjects) {}
	};
	private ReadPreference readPreference = ReadPreference.primary();
	private boolean exportExceptionHandleMBean = true;
	
	private final MongoClient mongoClient;
	private final MirroredDocuments mirroredDocuments;
	private String databaseName;
	
	@Autowired
	public YmerFactory(MongoClient mongoClient, MirroredDocumentsFactory mirroredDocumentsFactory) {
		this.mongoClient = mongoClient;
		this.mirroredDocuments = new MirroredDocuments(mirroredDocumentsFactory.getDocuments());
	}
	
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public void setExportExceptionHandlerMBean(boolean exportExceptionHandleMBean) {
		this.exportExceptionHandleMBean = exportExceptionHandleMBean;
	}
	
	/**
	 * Sets a MirrorExceptionListener (optional). <p>
	 * 
	 * @param exceptionListener
	 */
	public void setExceptionListener(MirrorExceptionListener exceptionListener) {
		this.exceptionListener = exceptionListener;
	}

	public SpaceDataSource createSpaceDataSource() {
		return new YmerSpaceDataSource(createSpaceMirrorContext());
	}

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		YmerSpaceSynchronizationEndpoint ymerSpaceSynchronizationEndpoint = new YmerSpaceSynchronizationEndpoint(createSpaceMirrorContext());
		if (this.exportExceptionHandleMBean) {
			ymerSpaceSynchronizationEndpoint.registerExceptionHandlerMBean();
		}
		return ymerSpaceSynchronizationEndpoint;
	}
	
	private SpaceMirrorContext createSpaceMirrorContext() {
		if (databaseName == null) {
			throw new IllegalStateException("The databasename property is mandatory. Use YmerFactory.setDatabaseName to configure the target database name");
		}
		SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongoClient, databaseName);
		MappingMongoConverter mongoConverter = new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		DocumentDb documentDb = DocumentDb.mongoDb(mongoClient.getDB(databaseName), readPreference);
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(mongoConverter);
		// Set the event publisher to null to avoid deadlocks when loading data in parallel
		if (mongoConverter.getMappingContext() instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware)mongoConverter.getMappingContext()).setApplicationEventPublisher(null);
		}
		SpaceMirrorContext mirrorContext = new SpaceMirrorContext(mirroredDocuments, documentConverter, documentDb, exceptionListener);
		return mirrorContext;
	}
	
}
