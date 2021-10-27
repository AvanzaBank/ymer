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

import static com.avanza.ymer.MirroredObject.DOCUMENT_INSTANCE_ID;
import static com.avanza.ymer.MirroredObject.DOCUMENT_ROUTING_KEY;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.stream.IntStream;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.client.MongoCollection;

public class PersistedInstanceIdRecalculationServiceTest {

	private static final String COLLECTION_NAME = "test_collection";

	@ClassRule
	public static final MirrorEnvironment mirrorEnvironment = new MirrorEnvironment();

	private MongoCollection<Document> collection;

	private final PersistedInstanceIdRecalculationService target;

	public PersistedInstanceIdRecalculationServiceTest() {
		TestSpaceMirrorFactory testSpaceMirrorFactory = new TestSpaceMirrorFactory(mirrorEnvironment.getMongoTemplate().getMongoDbFactory());
		SpaceSynchronizationEndpoint spaceSynchronizationEndpoint = testSpaceMirrorFactory.createSpaceSynchronizationEndpoint();
		target = ((YmerSpaceSynchronizationEndpoint) spaceSynchronizationEndpoint).getPersistedInstanceIdRecalculationService();
	}

	@Before
	public void setUp() {
		collection = mirrorEnvironment.getMongoTemplate().getCollection(COLLECTION_NAME);
		List<Document> documents = IntStream.rangeClosed(1, 1_000)
				.mapToObj(this::createDocument)
				.collect(toList());
		collection.insertMany(documents);
	}

	@After
	public void tearDown() {
		mirrorEnvironment.reset();
	}

	@Test
	public void shouldUpdateDocumentsWithCorrectInstanceId() {
		int numberOfInstances = 20;

		target.recalculatePersistedInstanceId(COLLECTION_NAME, numberOfInstances);

		MongoDocumentCollection mongoDocumentCollection = new MongoDocumentCollection(collection);
		mongoDocumentCollection.findAll()
				.forEach(document ->
								 assertThat(document.getInteger(DOCUMENT_INSTANCE_ID), equalTo(getInstanceId(document.get(DOCUMENT_ROUTING_KEY), numberOfInstances)))
				);
	}

	private Document createDocument(int id) {
		Document document = new Document("_id", id);
		document.put(DOCUMENT_ROUTING_KEY, id);
		return document;
	}
}