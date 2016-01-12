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

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.avanza.ymer.mirror.MirroredDocument.Flag;
import com.avanza.ymer.mirror.VersionedMongoDbExternalDataSourceInitialLoadIntegrationTest.TestSpaceObjectV1Patch;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.Mongo;

public class TestSpaceMirrorFactory {
	
	private String databaseName;
	private Mongo mongo;
	
	public static MirroredDocuments getMirroredDocuments() {
		return new MirroredDocuments(
			new MirroredDocument<>(TestSpaceObject.class, new TestSpaceObjectV1Patch()),
			MirroredDocument.createDocument(TestSpaceOtherObject.class, Collections.singleton(Flag.DoNotWriteBackPatchedDocuments), new TestSpaceObjectV1Patch())
		);
	}
	
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	@Autowired
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}
	
	public SpaceDataSource createSpaceDataSource() {
		SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongo, databaseName);
		MappingMongoConverter mongoConverter = new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		MirroredDocuments mirroredDocuments = getMirroredDocuments();
		MongoDbExternalDatasourceFactory factory = new MongoDbExternalDatasourceFactory(mirroredDocuments, mongo.getDB(databaseName), mongoConverter);
		return factory.createSpaceDataSource();
	}

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		SimpleMongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(mongo, databaseName);
		MappingMongoConverter mongoConverter = new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		MirroredDocuments mirroredDocuments = getMirroredDocuments();
		MongoDbExternalDatasourceFactory factory = new MongoDbExternalDatasourceFactory(mirroredDocuments, mongo.getDB(databaseName), mongoConverter);
		return factory.createSpaceSynchronizationEndpoint();
	}
	
}
