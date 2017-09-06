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

import com.avanza.ymer.YmerInitialLoadIntegrationTest.TestSpaceObjectV1Patch;
import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;


public class MongoPartitionFilterTest {

	private final MirroredObject<TestSpaceObject> mirroredObject = 
		MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(new TestSpaceObjectV1Patch()).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

	public final Fongo mongoRule = new Fongo("db");

	@Test
	public void canQueryAllNumbers() throws Exception {
		final int NUM_PARTITIONS = 13;
		final int ID_LOWER_BOUND = -100;
		final int ID_UPPER_BOUND = 100;

		DBCollection collection = mongoRule.getMongo().getDB("_test").getCollection("testCollection");
		for (int i = ID_LOWER_BOUND; i < ID_UPPER_BOUND; i++) {
			collection.insert(BasicDBObjectBuilder.start("_id", i).add("_routingKey", ((Integer)i).hashCode()).get());
		}

		Set<Integer> found = new HashSet<>();
		for (int i = 1; i <= NUM_PARTITIONS; i++) {
			DBCursor cur = collection.find(MongoPartitionFilter.create(SpaceObjectFilter.partitionFilter(mirroredObject, i, NUM_PARTITIONS)).toDBObject());
			found.addAll(extractIds(cur.toArray()));
		}

		for (int i = ID_LOWER_BOUND; i < ID_UPPER_BOUND; i++) {
			assertTrue(i + " not found!", found.contains(i));
		}
	}

	private Collection<? extends Integer> extractIds(List<DBObject> array) {
		Collection<Integer> result = new HashSet<>();
		for (DBObject e : array) {
			result.add((Integer)e.get("_id"));
		}
		return result;
	}

}
