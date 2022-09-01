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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import com.mongodb.BasicDBObject;

public class IdValidatorImplTest {
	private final MirrorEnvironment mirrorEnvironment = new MirrorEnvironment();
	private final IdValidatorImpl idValidator = spy(new IdValidatorImpl("collectionName"));
	private DocumentCollection collection;

	@Before
	public void beforeEachTest() {
		collection = new MongoDocumentCollection(
				mirrorEnvironment.getMongoDb()
						.getCollection("collectionName"),
				idValidator
		);
	}

	@Test
	public void shouldNotWarnWhenHandlingObjectsWithValidIdFields() {
		BasicDBObject obj1 = createNewObject("id1");
		BasicDBObject obj2 = createNewObject("id2");

		// Act
		collection.insert(obj1);
		collection.update(obj1);
		collection.replace(obj1, obj1);
		collection.replace(obj1, obj2);
		collection.delete(obj2);

		// Assert
		verify(idValidator, never()).warnAboutMissingIdField(any());
		verify(idValidator, never()).warnAboutNoDocumentMatch(any(), any());
	}

	@Test
	public void shouldWarnAboutMissingIdFields() {
		// Arrange
		BasicDBObject obj = createNewObject("id1");

		// Act
		collection.insert(createNewObjectWithoutId());
		collection.update(createNewObjectWithoutId());
		collection.replace(obj, createNewObjectWithoutId());
		collection.delete(createNewObjectWithoutId());

		// Assert
		verify(idValidator).warnAboutMissingIdField(eq("insert"));
		verify(idValidator).warnAboutMissingIdField(eq("update"));
		verify(idValidator).warnAboutMissingIdField(eq("replace"));
		verify(idValidator).warnAboutMissingIdField(eq("delete"));
	}

	@Test
	public void shouldWarnWhenUpdatingObjectIdThatDoesNotExist() {
		BasicDBObject obj = createNewObject("does-not-exist");

		// Act
		collection.update(obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("update"), eq("does-not-exist"));
	}

	@Test
	public void shouldWarnWhenUpdatingObjectWithoutId() {
		BasicDBObject obj = createNewObjectWithoutId();

		// Act
		collection.update(obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("update"), eq(null));
	}

	@Test
	public void shouldWarnWhenReplacingObjectIdThatDoesNotExist() {
		BasicDBObject obj = createNewObject("does-not-exist");

		// Act
		collection.replace(obj, obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("replace"), eq("does-not-exist"));
	}

	@Test
	public void shouldWarnWhenReplacingNewObjectIdThatDoesNotExist() {
		BasicDBObject oldObj = createNewObject("does-not-exist");
		BasicDBObject newObj = createNewObject("new-id");

		// Act
		collection.replace(oldObj, newObj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("replace"), eq("does-not-exist"));
	}

	@Test
	public void shouldWarnWhenDeletingObjectIdThatDoesNotExist() {
		// Arrange
		BasicDBObject obj = createNewObject("does-not-exist");

		// Act
		collection.delete(obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("delete"), eq("does-not-exist"));
	}

	private BasicDBObject createNewObject(String id) {
		return new BasicDBObject("_id", id);
	}

	private BasicDBObject createNewObjectWithoutId() {
		return new BasicDBObject();
	}
}
