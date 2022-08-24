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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class IdValidatorImplTest {

	@ClassRule
	public static final MirrorEnvironment mirrorEnvironment = new MirrorEnvironment();

	private final IdValidatorImpl idValidator = spy(new IdValidatorImpl("collectionName"));
	private DocumentCollection collection;

	@Before
	public void beforeEachTest() {
		collection = new MongoDocumentCollection(
				mirrorEnvironment.getMongoTemplate().getCollection("collectionName"),
				idValidator
		);
	}

	@After
	public void tearDown() {
		mirrorEnvironment.reset();
	}

	@Test
	public void shouldNotWarnWhenHandlingObjectsWithValidIdFields() {
		Document obj1 = createNewObject("id1");
		Document obj2 = createNewObject("id2");

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
		Document obj = createNewObject("id1");

		// Act
		collection.insert(createNewObjectWithoutId());
		collection.update(createNewObjectWithoutId());
		collection.replace(obj, createNewObjectWithoutId());
		collection.delete(createNewObjectWithoutId());

		// Assert
		verify(idValidator, times(2)).warnAboutMissingIdField(eq("insert"));
		verify(idValidator).warnAboutMissingIdField(eq("update"));
		verify(idValidator).warnAboutMissingIdField(eq("replace"));
		verify(idValidator).warnAboutMissingIdField(eq("delete"));
	}

	@Test
	public void shouldWarnWhenUpdatingObjectIdThatDoesNotExist() {
		Document obj = createNewObject("does-not-exist");

		// Act
		collection.update(obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("update"), eq("does-not-exist"));
	}

	@Test
	public void shouldWarnWhenUpdatingObjectWithoutId() {
		Document obj = createNewObjectWithoutId();

		// Act
		collection.update(obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("update"), eq(null));
	}

	@Test
	public void shouldWarnWhenReplacingObjectIdThatDoesNotExist() {
		Document obj = createNewObject("does-not-exist");

		// Act
		collection.replace(obj, obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("replace"), eq("does-not-exist"));
	}

	@Test
	public void shouldWarnWhenReplacingNewObjectIdThatDoesNotExist() {
		Document oldObj = createNewObject("does-not-exist");
		Document newObj = createNewObject("new-id");

		// Act
		collection.replace(oldObj, newObj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("replace"), eq("does-not-exist"));
	}

	@Test
	public void shouldWarnWhenDeletingObjectIdThatDoesNotExist() {
		// Arrange
		Document obj = createNewObject("does-not-exist");

		// Act
		collection.delete(obj);

		// Assert
		verify(idValidator).warnAboutNoDocumentMatch(eq("delete"), eq("does-not-exist"));
	}

	private Document createNewObject(String id) {
		return new Document("_id", id);
	}

	private Document createNewObjectWithoutId() {
		return new Document();
	}
}
