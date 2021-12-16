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
import static com.avanza.ymer.TestSpaceMirrorObjectDefinitions.TEST_SPACE_OBJECT;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;
import static com.j_spaces.core.Constants.Mirror.FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
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

			verifyCollection(numberOfInstances);
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

				await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verifyCollection(numberOfInstances));
			}
		}, new SystemProperties(testProperties));
	}

	@Test
	public void shouldRecalculateInstanceIdUsingNumberOfInstancesFromSystemProperty() throws Exception {
		int numberOfInstances = 32;

		try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
			PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
			execute(() -> {
				target.recalculatePersistedInstanceId();
				verifyCollection(numberOfInstances);
			}, new SystemProperties("cluster.partitions", String.valueOf(numberOfInstances)));
		}
	}

	@Test
	public void shouldThrowExceptionWhenNumberOfInstancesCannotBeDetermined() {
		try (YmerSpaceSynchronizationEndpoint endpoint = createSpaceSynchronizationEndpoint()) {
			PersistedInstanceIdRecalculationService target = endpoint.getPersistedInstanceIdRecalculationService();
			assertThrows(IllegalStateException.class, target::recalculatePersistedInstanceId);
		}
	}

	private static YmerSpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		TestSpaceMirrorFactory testSpaceMirrorFactory = new TestSpaceMirrorFactory(mirrorEnvironment.getMongoTemplate().getMongoDbFactory());
		return (YmerSpaceSynchronizationEndpoint) testSpaceMirrorFactory.createSpaceSynchronizationEndpoint();
	}

	private void verifyCollection(int numberOfInstances) {
		MongoDocumentCollection mongoDocumentCollection = new MongoDocumentCollection(collection);
		mongoDocumentCollection.findAll()
				.forEach(document ->
								 assertThat(document.getInteger(DOCUMENT_INSTANCE_ID), is(getInstanceId(document.get(DOCUMENT_ROUTING_KEY), numberOfInstances)))
				);
		assertThat(mongoDocumentCollection.getIndexes().collect(toList()),
						   hasItem(both(isIndexForField(DOCUMENT_INSTANCE_ID)).and(nameEndsWith("_" + numberOfInstances))));
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

	private static Matcher<IndexInfo> nameEndsWith(String nameSuffix) {
		return new CustomTypeSafeMatcher<>("index name ends with " + nameSuffix) {
			@Override
			protected boolean matchesSafely(IndexInfo item) {
				return item.getName().endsWith(nameSuffix);
			}
		};
	}

}