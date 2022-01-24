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
import static com.avanza.ymer.PersistedInstanceIdUtil.getInstanceIdFieldName;
import static com.avanza.ymer.TestSpaceMirrorObjectDefinitions.TEST_SPACE_OBJECT;
import static com.avanza.ymer.TestSpaceMirrorObjectDefinitions.TEST_SPACE_OTHER_OBJECT;
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

public class PersistedInstanceIdCalculationServiceTest {

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
	public void shouldCalculateInstanceIdUsingNumberOfInstancesFromSpaceProperty() throws Exception {
		int numberOfInstances = 16;

		try(RunningPu mirrorPu = PuConfigurers.mirrorPu("classpath:/test-mirror-pu.xml")
				.contextProperty("exportExceptionHandlerMBean", "true")
				.contextProperty(FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, String.valueOf(numberOfInstances))
				.parentContext(mirrorEnvironment.getMongoClientContext())
				.configure()) {
			mirrorPu.start();

			BeanFactory applicationContext = mirrorPu.getApplicationContexts().iterator().next();
			YmerSpaceSynchronizationEndpoint spaceSynchronizationEndpoint = applicationContext.getBean(YmerSpaceSynchronizationEndpoint.class);
			PersistedInstanceIdCalculationService persistedInstanceIdCalculationService = spaceSynchronizationEndpoint.getPersistedInstanceIdCalculationService();

			persistedInstanceIdCalculationService.calculatePersistedInstanceId();

			verifyCollectionIsCalculatedFor(numberOfInstances);
			verifyStatistics(TEST_SPACE_OBJECT, persistedInstanceIdCalculationService, new int[] { 16 });
			assertThat(persistedInstanceIdCalculationService.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { 16 }));
		}
	}

	@Test
	public void shouldStartCalculationJobOnStartup() throws Exception {
		int numberOfInstances = 14;
		Properties testProperties = new Properties();
		testProperties.setProperty("ymer.com.avanza.ymer.TestSpaceObject.triggerInstanceIdCalculationOnStartup", "true");
		testProperties.setProperty("ymer.com.avanza.ymer.TestSpaceObject.triggerInstanceIdCalculationWithDelay", "1");

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
	public void shouldCalculateInstanceIdUsingNumberOfInstancesFromSystemProperty() throws Exception {
		int numberOfInstances = 32;
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdCalculationService target = endpoint.getPersistedInstanceIdCalculationService();
				target.calculatePersistedInstanceId();
				verifyCollectionIsCalculatedFor(numberOfInstances);
				verifyStatistics(TEST_SPACE_OBJECT, target, new int[] { numberOfInstances });
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { numberOfInstances }));
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
	}

	@Test
	public void verifyAllCollectionsAreReadyStatistic() throws Exception {
		int numberOfInstances = 22;
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdCalculationService target = endpoint.getPersistedInstanceIdCalculationService();

				target.calculatePersistedInstanceId(TEST_SPACE_OBJECT.collectionName());
				verifyStatistics(TEST_SPACE_OBJECT, target, new int[] { 22 });

				// TEST_SPACE_OTHER_OBJECT is not calculated yet, so this should be empty
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { }));

				target.calculatePersistedInstanceId(TEST_SPACE_OTHER_OBJECT.collectionName());
				verifyStatistics(TEST_SPACE_OTHER_OBJECT, target, new int[] { 22 });

				// Now that all collections are calculated, this should contain number of instances
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { 22 }));
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
	}

	@Test
	public void testCalculatingInstanceIdForNextNumberOfInstances() throws Exception {
		int currentNumberOfInstances = 32;

		TestSpaceMirrorFactory testSpaceMirrorFactory = new TestSpaceMirrorFactory(mirrorEnvironment.getMongoTemplate().getMongoDbFactory());
		testSpaceMirrorFactory.setNextNumberOfInstances(38);
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = (YmerSpaceSynchronizationEndpoint) testSpaceMirrorFactory.createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdCalculationService target = endpoint.getPersistedInstanceIdCalculationService();

				// Verify statistics are empty before calculation
				verifyStatistics(TEST_SPACE_OBJECT, target, new int[] { });
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { }));

				target.calculatePersistedInstanceId();
				verifyCollectionIsCalculatedFor(32);
				verifyCollectionIsCalculatedFor(38);
				verifyStatistics(TEST_SPACE_OBJECT, target, new int[] { 32, 38 });
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { 32, 38 }));

				// Recalculate using a different next number of instances
				testSpaceMirrorFactory.setNextNumberOfInstances(40);
				target.calculatePersistedInstanceId();

				verifyCollectionIsCalculatedFor(32);
				verifyCollectionIsNotCalculatedFor(38);
				verifyCollectionIsCalculatedFor(40);
				verifyStatistics(TEST_SPACE_OBJECT, target, new int[] { 32, 40 });
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { 32, 40 }));

				// Recalculate without any next number of instances, deleting next instance id field
				testSpaceMirrorFactory.setNextNumberOfInstances(null);
				target.calculatePersistedInstanceId();

				verifyCollectionIsCalculatedFor(32);
				verifyCollectionIsNotCalculatedFor(40);
				verifyStatistics(TEST_SPACE_OBJECT, target, new int[] { 32 });
				assertThat(target.getNumberOfPartitionsThatDataIsPreparedFor(), is(new int[] { 32 }));
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(currentNumberOfInstances)));
	}

	@Test
	public void shouldThrowExceptionWhenNumberOfInstancesCannotBeDetermined() {
		try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
			PersistedInstanceIdCalculationService target = endpoint.getPersistedInstanceIdCalculationService();
			assertThrows(IllegalStateException.class, target::calculatePersistedInstanceId);
		}
	}

	@Test
	public void testCollectionNeedsCalculation() throws Exception {
		int numberOfInstances = 1;
		var spaceMirrorFactory = new TestSpaceMirrorFactory(mirrorEnvironment.getMongoTemplate().getMongoDbFactory());
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = (YmerSpaceSynchronizationEndpoint) spaceMirrorFactory.createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdCalculationService target = endpoint.getPersistedInstanceIdCalculationService();
				assertTrue("In the initial state, collection should need to be calculated",
						target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));

				target.calculatePersistedInstanceId(TEST_SPACE_OBJECT.collectionName());
				assertFalse("The collection should now be calculated for [1] number of instances",
						target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));

				spaceMirrorFactory.setNextNumberOfInstances(2);
				assertTrue("Should need recalculation when next number of instances is set",
						target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));

				target.calculatePersistedInstanceId(TEST_SPACE_OBJECT.collectionName());
				assertFalse("The collection should now be calculated for [1, 2] number of instances",
						target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));

				spaceMirrorFactory.setNextNumberOfInstances(3);
				assertTrue("Should need recalculation again when next number of instances is updated to a different value",
						target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));

				spaceMirrorFactory.setNextNumberOfInstances(null);
				assertFalse("Should not need recalculation if next number of instances property is removed",
						target.collectionNeedsCalculation(TEST_SPACE_OBJECT.collectionName()));
			}
		}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
	}

	@Test
	public void notExistingCollectionShouldReturnFalse() throws Exception {
		int numberOfInstances = 1;
		execute(() -> {
			try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
				PersistedInstanceIdCalculationService target = endpoint.getPersistedInstanceIdCalculationService();
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
		String fieldName = getInstanceIdFieldName(numberOfInstances);
		mongoDocumentCollection.findAll()
				.forEach(document ->
								 assertThat(document.getInteger(fieldName), is(getInstanceId(document.get(DOCUMENT_ROUTING_KEY), numberOfInstances)))
				);
		assertThat(mongoDocumentCollection.getIndexes().collect(toList()), hasItem(isIndexForField(fieldName)));
	}

	private void verifyCollectionIsNotCalculatedFor(int numberOfInstances) {
		MongoDocumentCollection mongoDocumentCollection = new MongoDocumentCollection(collection);
		String fieldName = getInstanceIdFieldName(numberOfInstances);
		mongoDocumentCollection.findAll()
				.forEach(document ->
						assertThat("Should not contain " + fieldName, document.containsKey(fieldName), is(false))
				);
		assertThat(mongoDocumentCollection.getIndexes().collect(toList()), not(hasItem(isIndexForField(fieldName))));
	}

	private static void verifyStatistics(MirroredObjectDefinition<?> mirroredObject,
			PersistedInstanceIdCalculationService calculationService,
			int[] readyForNumberOfPartitions) {
		MirroredObject<?> testObject = mirroredObject.buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		// Verify statistics are collected properly
		PersistedInstanceIdStatisticsMBean statistics = calculationService.collectStatistics(testObject);
		assertThat(statistics.isCalculationInProgress(), is(false));
		assertThat(statistics.getNumberOfPartitionsThatCollectionIsPreparedFor(), is(readyForNumberOfPartitions));
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