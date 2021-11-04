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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.openspaces.core.cluster.ClusterInfo;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.avanza.ymer.MongoDocumentCollectionTest.FakeSpaceObject;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class YmerFactoryTest {
	private final MongoDatabase db = mock(MongoDatabase.class);
	@SuppressWarnings("unchecked")
	private final MongoCollection<Document> fakeSpaceObjectCollection = createMockedEmptyCollection();
	@SuppressWarnings("unchecked")
	private final MongoCollection<Document> testSpaceObjectCollection = createMockedEmptyCollection();

	@Before
	public void beforeEachTest() {
		when(db.getCollection("fakeSpaceObject")).thenReturn(fakeSpaceObjectCollection);
		when(db.getCollection("testSpaceObject")).thenReturn(testSpaceObjectCollection);
	}

	@Test
	public void shouldSetReadPreferenceOnCreatedDocumentCollections() {
		// Arrange
		final Collection<MirroredObjectDefinition<?>> definitions = Arrays.asList(
				MirroredObjectDefinition.create(TestSpaceObject.class),
				MirroredObjectDefinition.create(FakeSpaceObject.class)
						.withReadPreference(ReadPreference.secondaryPreferred())
		);
		final YmerFactory factory = new YmerFactory(createMockedFactory(db),
													mock(MongoConverter.class),
													definitions)
				.withReadPreference(ReadPreference.primaryPreferred());

		final YmerSpaceDataSource ysds = (YmerSpaceDataSource) factory.createSpaceDataSource();
		ysds.setClusterInfo(new ClusterInfo("schema", 1, 1, 1, 1));

		// Act
		ysds.initialDataLoad().forEachRemaining(new ArrayList<>()::add);

		// Assert
		final ArgumentCaptor<ReadPreference> testSpaceReadPreferenceCaptor = ArgumentCaptor.forClass(ReadPreference.class);
		final ArgumentCaptor<ReadPreference> fakeSpaceReadPreferenceCaptor = ArgumentCaptor.forClass(ReadPreference.class);
		verify(testSpaceObjectCollection).withReadPreference(testSpaceReadPreferenceCaptor.capture());
		verify(fakeSpaceObjectCollection).withReadPreference(fakeSpaceReadPreferenceCaptor.capture());

		// We have explicitly set the readPreference on the "FakeSpaceObject" collection,
		// so this should have been used for that:
		assertThat(fakeSpaceReadPreferenceCaptor.getValue(), equalTo(ReadPreference.secondaryPreferred()));
		// But for the "TestSpaceObject" collection, we should use the default
		// from YmerFactory.
		assertThat(testSpaceReadPreferenceCaptor.getValue(), equalTo(ReadPreference.primaryPreferred()));
	}

	private MongoDbFactory createMockedFactory(MongoDatabase db) {
		MongoDbFactory mongoDbFactory = mock(MongoDbFactory.class);
		when(mongoDbFactory.getDb()).thenReturn(db);
		return mongoDbFactory;
	}

	private MongoCollection createMockedEmptyCollection() {
		MongoCollection collection = mock(MongoCollection.class);
		MongoCursor mongoCursor = mock(MongoCursor.class);
		FindIterable findIterable = mock(FindIterable.class);

		doReturn(false).when(mongoCursor).hasNext();
		doReturn(mongoCursor).when(findIterable).iterator();
		doCallRealMethod().when(findIterable).spliterator();
		doReturn(findIterable).when(collection).find();
		doReturn(new MongoNamespace("test.1")).when(collection).getNamespace();
		return collection;
	}
}
