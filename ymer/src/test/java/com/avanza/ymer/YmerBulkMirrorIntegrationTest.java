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

import static com.avanza.ymer.TestSpaceMirrorObjectDefinitions.TEST_SPACE_OBJECT;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import org.junit.Test;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.gs.test.RunningPu;
import com.gigaspaces.client.WriteModifiers;

/**
 * These tests use {@link BulkMirroredObjectWriter} for persisting to MongoDB.
 */
public class YmerBulkMirrorIntegrationTest extends YmerMirrorIntegrationTestBase {

	@Override
	void configureTestCase(RunningPu pu, RunningPu mirrorPu) {
		TestSpaceMirrorFactory spaceMirrorFactory = mirrorPu.getPrimaryApplicationContexts().iterator().next().getBean(TestSpaceMirrorFactory.class);
		spaceMirrorFactory.setUseBulkWrites(true);
	}

	@Test
	public void shouldContinueWritingAfterOneMessageFails() {
		TestSpaceObject object1 = new TestSpaceObject("id_1", "message1");
		TestSpaceObject object2 = new TestSpaceObject("id_2", "message2");

		// This will cause the insert to fail on this object as it already exists in mongo when added to the gigaspace
		mongo.insert(object1, TEST_SPACE_OBJECT.collectionName());

		// This object would not be written if no retries are made
		TestSpaceObject object3 = new TestSpaceObject("id_3", "message3");

		gigaSpace.writeMultiple(new TestSpaceObject[] {object1, object2, object3}, WriteModifiers.WRITE_ONLY);

		await().until(() -> mongo.findAll(TestSpaceObject.class), containsInAnyOrder(object1, object2, object3));
	}

	@Test
	public void shouldUpsertMissingUpdateWithWarning() {
		TestSpaceObject object1 = new TestSpaceObject("id_1", "message1");
		TestSpaceObject object2 = new TestSpaceObject("id_2", "message2");

		gigaSpace.writeMultiple(new TestSpaceObject[] {object1, object2}, WriteModifiers.WRITE_ONLY);
		await().until(() -> mongo.findAll(TestSpaceObject.class), hasItems(object1, object2));

		mongo.remove(new Query().addCriteria(where("_id").is("id_1")), TEST_SPACE_OBJECT.collectionName());

		object2.setMessage("test");

		// This triggers an update on an object that does not exist in database anymore, which will cause it to
		// be inserted again with an upsert
		gigaSpace.writeMultiple(new TestSpaceObject[] {object1, object2}, WriteModifiers.UPDATE_ONLY);

		// The logs should contain a warning like
		// "Tried to update 2 documents in current bulk write, but only 1 were matched by query.
		// MongoDB and space seems to be out of sync!
		// The following ids were inserted into MongoDB as a result of update operations: [id_1]."
		await().until(() -> mongo.findAll(TestSpaceObject.class), containsInAnyOrder(object1, object2));
	}

}
