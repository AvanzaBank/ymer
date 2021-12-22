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

import static com.avanza.ymer.MirroredObject.DOCUMENT_ROUTING_KEY;
import static com.avanza.ymer.TestSpaceMirrorObjectDefinitions.TEST_SPACE_OBJECT;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;
import static com.j_spaces.core.Constants.Mirror.FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static uk.org.webcompere.systemstubs.SystemStubs.execute;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.bson.Document;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.mongodb.core.index.IndexInfo;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.mongodb.client.MongoCollection;

import uk.org.webcompere.systemstubs.properties.SystemProperties;

public class PersistedInstanceIdRecalculationServiceTest {

	@ClassRule
	public static final MirrorEnvironment mirrorEnvironment = new MirrorEnvironment();

	private final MongoCollection<Document> collection = mirrorEnvironment.getMongoTemplate().getCollection(TEST_SPACE_OBJECT.collectionName());

	@Before
	public void setUp() {
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
	public void shouldRecalculateInstanceIdUsingNumberOfInstancesFromSpaceProperty() throws Exception {
		int numberOfInstances = 16;

		try(RunningPu mirrorPu = PuConfigurers.mirrorPu("classpath:/test-mirror-pu.xml")
				.contextProperty("exportExceptionHandlerMBean", "true")
				.contextProperty(FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, String.valueOf(numberOfInstances))
				.parentContext(mirrorEnvironment.getMongoClientContext())
				.configure()) {
			mirrorPu.start();

			BeanFactory applicationContext = mirrorPu.getApplicationContexts().iterator().next();
			YmerSpaceSynchronizationEndpoint spaceSynchronizationEndpoint = applicationContext.getBean(YmerSpaceSynchronizationEndpoint.class);
			PersistedInstanceIdRecalculationService persistedInstanceIdRecalculationService = spaceSynchronizationEndpoint.getPersistedInstanceIdRecalculationService();

			persistedInstanceIdRecalculationService.recalculatePersistedInstanceId();

			verifyCollectionIsCalculatedFor(numberOfInstances);
		}
	}

	@Test
	public void shouldStartRecalculationJobOnStartup() throws Exception {
		int numberOfInstances = 14;
		Properties testProperties = new Properties();
		testProperties.setProperty("ymer.com.avanza.ymer.TestSpaceObject.recalculateInstanceIdOnStartup", "true");
		testProperties.setProperty("ymer.com.avanza.ymer.TestSpaceObject.recalculateInstanceIdWithDelay", "1");

		execute(() -> {
			try (RunningPu mirrorPu = PuConfigurers.mirrorPu("classpath:/test-mirror-pu.xml")
					.contextProperty("exportExceptionHandlerMBean", "false")
					.contextProperty(FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, String.valueOf(numberOfInstances))
					.parentContext(mirrorEnvironment.getMongoClientContext())
					.configure()) {
				mirrorPu.start();

				await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verifyCollectionIsCalculatedFor(numberOfInstances));
			}
		}, new SystemProperties(testProperties));
	}

	@Test
	public void shouldRecalculateInstanceIdUsingNumberOfInstancesFromSystemProperty() throws Exception {
		int numberOfInstances = 32;
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
				target.recalculatePersistedInstanceId();
				verifyCollectionIsCalculatedFor(numberOfInstances);
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
	}

	@Test
	public void testRecalculatingInstanceIdForNextNumberOfInstances() throws Exception {
		int currentNumberOfInstances = 32;

		TestSpaceMirrorFactory testSpaceMirrorFactory = new TestSpaceMirrorFactory(mirrorEnvironment.getMongoTemplate().getMongoDbFactory());
		testSpaceMirrorFactory.setNextNumberOfInstances(38);
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = (YmerSpaceSynchronizationEndpoint) testSpaceMirrorFactory.createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
				target.recalculatePersistedInstanceId();
				verifyCollectionIsCalculatedFor(32);
				verifyCollectionIsCalculatedFor(38);

				// Recalculate again, using a different amount of instances
				testSpaceMirrorFactory.setNextNumberOfInstances(40);
				target.recalculatePersistedInstanceId();

				verifyCollectionIsCalculatedFor(32);
				verifyCollectionIsNotCalculatedFor(38);
				verifyCollectionIsCalculatedFor(40);

				// Recalculate again, without any number of instances, deleting next instance id field
				testSpaceMirrorFactory.setNextNumberOfInstances(null);
				target.recalculatePersistedInstanceId();

				verifyCollectionIsCalculatedFor(32);
				verifyCollectionIsNotCalculatedFor(40);
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(currentNumberOfInstances)));
	}

	@Test
	public void shouldThrowExceptionWhenNumberOfInstancesCannotBeDetermined() {
		try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
			PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
			assertThrows(IllegalStateException.class, target::recalculatePersistedInstanceId);
		}
	}

	@Test
	public void testCollectionNeedsRecalculation() throws Exception {
		int numberOfInstances = 1;
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
				assertTrue(target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));

				target.recalculatePersistedInstanceId(TEST_SPACE_OBJECT.collectionName());
				verifyCollectionIsCalculatedFor(numberOfInstances);

				assertFalse(target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
	}

	@Test
	public void notExistingCollectionShouldReturnFalse() throws Exception {
		int numberOfInstances = 1;
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
				assertFalse(target.collectionNeedsCalculation("NOT_EXISTING_COLLECTION"));
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
	}

	private YmerSpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		TestSpaceMirrorFactory testSpaceMirrorFactory = new TestSpaceMirrorFactory(mirrorEnvironment.getMongoTemplate().getMongoDbFactory());
		return (YmerSpaceSynchronizationEndpoint) testSpaceMirrorFactory.createSpaceSynchronizationEndpoint();
	}

	private void verifyCollectionIsCalculatedFor(int numberOfInstances) {
		MongoDocumentCollection mongoDocumentCollection = new MongoDocumentCollection(collection);
		String fieldName = PersistedInstanceIdUtil.getInstanceIdFieldName(numberOfInstances);
		mongoDocumentCollection.findAll()
				.forEach(document ->
								 assertThat(document.getInteger(fieldName), is(getInstanceId(document.get(DOCUMENT_ROUTING_KEY), numberOfInstances)))
				);
		assertThat(mongoDocumentCollection.getIndexes().collect(toList()), hasItem(isIndexForField(fieldName)));
	}

	private void verifyCollectionIsNotCalculatedFor(int numberOfInstances) {
		MongoDocumentCollection mongoDocumentCollection = new MongoDocumentCollection(collection);
		String fieldName = PersistedInstanceIdUtil.getInstanceIdFieldName(numberOfInstances);
		mongoDocumentCollection.findAll()
				.forEach(document ->
						assertThat("Should not contain " + fieldName, document.containsKey(fieldName), is(false))
				);
		assertThat(mongoDocumentCollection.getIndexes().collect(toList()), not(hasItem(isIndexForField(fieldName))));
	}

	private Document createDocument(int id) {
		Document document = new Document("_id", id);
		document.put(DOCUMENT_ROUTING_KEY, id);
		return document;
	}

	@SuppressWarnings("SameParameterValue")
	private static Matcher<IndexInfo> isIndexForField(String fieldName) {
		return new CustomTypeSafeMatcher<>("is index for field " + fieldName) {
			@Override
			protected boolean matchesSafely(IndexInfo item) {
				return item.isIndexForFields(List.of(fieldName));
			}
		};
	}

}