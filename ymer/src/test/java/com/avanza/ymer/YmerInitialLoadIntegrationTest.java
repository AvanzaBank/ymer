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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.avanza.ymer.MirroredDocument.Flag;
import com.mongodb.BasicDBObject;

public class YmerInitialLoadIntegrationTest {
	
	static MirroredDocument<TestSpaceObject> mirroredDocument = MirroredDocumentDefinition.create(TestSpaceObject.class).documentPatches(new TestSpaceObjectV1Patch()).buildMirroredDocument();
	static MirroredDocument<TestSpaceOtherObject> mirroredOtherDocument = MirroredDocumentDefinition.create(TestSpaceOtherObject.class)
																									.flags(Flag.DO_NOT_WRITE_BACK_PATCHED_DOCUMENTS) 
																									.documentPatches(new TestSpaceObjectV1Patch()).buildMirroredDocument();
	static MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument, mirroredOtherDocument);
	
	public static MirrorEnvironment mirrorEnv = new MirrorEnvironment();
	
	public RunningPu pu = PuConfigurers.partitionedPu("classpath:/test-pu.xml")
									   .numberOfPrimaries(1)
									   .startAsync(false)
									   .parentContext(mirrorEnv.getMongoClientContext())
									   .configure();
	@After
	public void cleanup() throws Exception {
		mirrorEnv.dropAllMongoCollections();
		pu.close();
	}

	@Test
	public void migratesOldDocumentOnInitialLoad() throws Exception {
		BasicDBObject spaceObjectV1 = new BasicDBObject();
		spaceObjectV1.put("_id", "id_v1");
		spaceObjectV1.put("message", "Msg_V1");
		
		BasicDBObject spaceObjectV2 = new BasicDBObject();
		spaceObjectV2.put("_id", "id_v2");
		spaceObjectV2.put("message", "Msg_V2");
		spaceObjectV2.put(MirroredDocument.DOCUMENT_FORMAT_VERSION_PROPERTY, 2);
		
		BasicDBObject spaceOtherObject = new BasicDBObject();
		spaceOtherObject.put("_id", "otherId");
		spaceOtherObject.put("message", "Msg_V1");
		
		MongoTemplate mongoTemplate = mirrorEnv.getMongoTemplate();
		mongoTemplate.getCollection(mirroredDocument.getCollectionName()).insert(spaceObjectV1);
		mongoTemplate.getCollection(mirroredDocument.getCollectionName()).insert(spaceObjectV2);
		mongoTemplate.getCollection(mirroredOtherDocument.getCollectionName()).insert(spaceOtherObject);

		pu.start();
		
		// Verify SpaceObject
		GigaSpace gigaSpace = pu.getClusteredGigaSpace();
		assertEquals(2, gigaSpace.count(new TestSpaceObject()));
		TestSpaceObject testSpaceObject = gigaSpace.readById(TestSpaceObject.class, "id_v1");
		assertEquals("patched_Msg_V1", testSpaceObject.getMessage());
		
		List<BasicDBObject> allDocs = mongoTemplate.findAll(BasicDBObject.class, mirroredDocument.getCollectionName());
		assertEquals(2, allDocs.size());
		assertEquals(2, mirroredDocument.getDocumentVersion(allDocs.get(0)));
		assertEquals(2, mirroredDocument.getDocumentVersion(allDocs.get(1)));
		
		// Verify SpaceOtherObject
		assertEquals(1, gigaSpace.count(new TestSpaceOtherObject()));
		TestSpaceOtherObject testSpaceOtherObject = gigaSpace.readById(TestSpaceOtherObject.class, "otherId");
		assertEquals("patched_Msg_V1", testSpaceOtherObject.getMessage());
		
		List<BasicDBObject> allOtherDocs = mongoTemplate.findAll(BasicDBObject.class, mirroredOtherDocument.getCollectionName());
		assertEquals(1, allOtherDocs.size());
		assertEquals(1, mirroredOtherDocument.getDocumentVersion(allOtherDocs.get(0)));
	}
	
	public static class TestSpaceObjectV1Patch implements DocumentPatch {
		@Override
		public void apply(BasicDBObject dbObject) {
			dbObject.put("message", "patched_" + dbObject.getString("message"));
		}

		@Override
		public int patchedVersion() {
			return 1;
		}
		
	}
	
}
