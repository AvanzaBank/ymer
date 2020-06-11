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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Spliterator;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.openspaces.core.cluster.ClusterInfo;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import com.avanza.ymer.MongoDocumentCollectionTest.FakeSpaceObject;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

public class YmerFactoryTest {
	private final DB db = mock(DB.class);
	private final DBCollection fakeSpaceObjectCollection = createMockedEmptyCollection();
	private final DBCollection testSpaceObjectCollection = createMockedEmptyCollection();

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
		final YmerFactory factory = new YmerFactory(createMockedFactory(db), mock(MongoConverter.class), definitions)
				.withReadPreference(ReadPreference.primaryPreferred());
		final YmerSpaceDataSource ysds = (YmerSpaceDataSource) factory.createSpaceDataSource();
		ysds.setClusterInfo(new ClusterInfo("schema", 1, 1, 1, 1));

		// Act
		ysds.initialDataLoad().forEachRemaining(new ArrayList<>()::add);

		// Assert
		final ArgumentCaptor<ReadPreference> testSpaceReadPreferenceCaptor = ArgumentCaptor.forClass(ReadPreference.class);
		final ArgumentCaptor<ReadPreference> fakeSpaceReadPreferenceCaptor = ArgumentCaptor.forClass(ReadPreference.class);
		verify(testSpaceObjectCollection).setReadPreference(testSpaceReadPreferenceCaptor.capture());
		verify(fakeSpaceObjectCollection).setReadPreference(fakeSpaceReadPreferenceCaptor.capture());

		// We have explicitly set the readPreference on the "FakeSpaceObject" collection,
		// so this should have been used for that:
		assertThat(fakeSpaceReadPreferenceCaptor.getValue(), equalTo(ReadPreference.secondaryPreferred()));
		// But for the "TestSpaceObject" collection, we should use the default
		// from YmerFactory.
		assertThat(testSpaceReadPreferenceCaptor.getValue(), equalTo(ReadPreference.primaryPreferred()));
	}

	private MongoDbFactory createMockedFactory(DB db) {
		final MongoDbFactory mongoDbFactory = mock(MongoDbFactory.class);
		when(mongoDbFactory.getDb()).thenReturn(db);
		return mongoDbFactory;
	}

	private DBCollection createMockedEmptyCollection() {
		final DBCollection collection = mock(DBCollection.class);
		doReturn(new DBCursor(collection, new BasicDBList(), null, null) {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public Spliterator<DBObject> spliterator() {
				return Stream.<DBObject>empty().spliterator();
			}
		}).when(collection).find();
		return collection;
	}
}
