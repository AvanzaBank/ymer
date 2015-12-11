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

import java.util.Collections;

import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import se.avanzabank.mongodb.support.mirror.MirroredDocument.Flag;
import se.avanzabank.mongodb.support.mirror.VersionedMongoDbExternalDataSourceInitialLoadIntegrationTest.TestSpaceObjectV1Patch;
import se.avanzabank.mongodb.util.LifecycleContainer;

public class TestSpaceMirrorFactory {
	
	private final LifecycleContainer lifecycleContainer = new LifecycleContainer();
	
	public static MirroredDocuments getMirroredDocuments() {
		return new MirroredDocuments(
			new MirroredDocument<>(TestSpaceObject.class, new TestSpaceObjectV1Patch()),
			MirroredDocument.createDocument(TestSpaceOtherObject.class, Collections.singleton(Flag.DoNotWriteBackPatchedDocuments), new TestSpaceObjectV1Patch())
		);
	}
	
	public ManagedDataSourceAndBulkDataPersister create(MongoDbFactory mongoDbFactory) {
		MappingMongoConverter mongoConverter = new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		MirroredDocuments mirroredDocuments = getMirroredDocuments();
		ManagedDataSourceAndBulkDataPersister factory = new MongoDbExternalDatasourceFactory(mirroredDocuments, mongoDbFactory, mongoConverter).create();
		lifecycleContainer.add(factory);
		return factory;
	}
	
	
}
