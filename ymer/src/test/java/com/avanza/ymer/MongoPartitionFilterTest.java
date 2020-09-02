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

import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.bson.Document;
import org.junit.Test;
import com.avanza.ymer.YmerInitialLoadIntegrationTest.TestSpaceObjectV1Patch;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;


public class MongoPartitionFilterTest {

	private final MirroredObject<TestSpaceObject> mirroredObject = 
		MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(new TestSpaceObjectV1Patch()).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

	private final MongoServer mongoServer;
	private final MongoClient mongoClient;

	public MongoPartitionFilterTest() {
		mongoServer = new MongoServer(new MemoryBackend());
		InetSocketAddress serverAddress = mongoServer.bind();
		mongoClient = new MongoClient(new ServerAddress(serverAddress));
	}

	@Test
	public void canQueryAllNumbers() throws Exception {
		final int NUM_PARTITIONS = 13;
		final int ID_LOWER_BOUND = -100;
		final int ID_UPPER_BOUND = 100;

		MongoCollection<Document> collection = mongoClient.getDatabase("_test")
														  .getCollection("testCollection");
		for (int i = ID_LOWER_BOUND; i < ID_UPPER_BOUND; i++) {
			collection.insertOne(new Document(Map.of("_id", i,
													 "_routingKey", ((Integer)i).hashCode())));
		}

		Set<Integer> found = new HashSet<>();
		for (int i = 1; i <= NUM_PARTITIONS; i++) {

			FindIterable<Document> documents =
					collection.find(MongoPartitionFilter
											.createBsonFilter(SpaceObjectFilter
																	  .partitionFilter(mirroredObject, i, NUM_PARTITIONS))
											.toBson());
			documents.forEach((Consumer<? super Document>)doc -> {
				Integer id = (Integer) doc.get("_id");
				found.add(id);
			});
		}

		for (int i = ID_LOWER_BOUND; i < ID_UPPER_BOUND; i++) {
			assertTrue(i + " not found!", found.contains(i));
		}
	}
}
