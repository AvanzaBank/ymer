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
package com.avanza.ymer.junit5;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.MirroredObjectTestHelper;
import com.avanza.ymer.TestDocumentConverter;
import com.mongodb.DB;
import com.mongodb.client.MongoDatabase;

/**
 * Base class for testing that objects may be marshalled to a mongo document and
 * then unmarshalled back into an object.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class YmerConverterTestBase {

	private final MongoDbFactory dummyMongoDbFactory;

	public YmerConverterTestBase() {
		this.dummyMongoDbFactory = new MongoDbFactory() {
			// The MongoDbFactory is never used during the tests.
			@Override
			public MongoDatabase getDb(String dbName) throws DataAccessException {
				return null;
			}

			@Override
			public MongoDatabase getDb() throws DataAccessException {
				return null;
			}

			@Override
			public PersistenceExceptionTranslator getExceptionTranslator() {
				return null;
			}

			@Override
			public DB getLegacyDb() {
				return null;
			}
		};
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@ParameterizedTest
	@MethodSource("testCases")
	void serializationTest(ConverterTest testCase) {
		Object spaceObject = testCase.spaceObject;
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(spaceObject.getClass());

		TestDocumentConverter documentConverter = TestDocumentConverter.create(createMongoConverter(dummyMongoDbFactory));
		Document basicDBObject = documentConverter.convertToBsonDocument(spaceObject);
		Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
		assertThat(reCreated, testCase.matcher);
	}

	@ParameterizedTest
	@MethodSource("testCases")
	void canMirrorSpaceObject(ConverterTest<?> testCase) {
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(testCase.spaceObject.getClass());
		assertTrue(mirroredDocument.isMirroredType(), "Mirroring of " + testCase.getClass());
	}

	@ParameterizedTest
	@MethodSource("testCases")
	void testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject(ConverterTest<?> testCase) {
		Object spaceObject = testCase.spaceObject;
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(spaceObject.getClass());

		TestDocumentConverter documentConverter = TestDocumentConverter.create(createMongoConverter(dummyMongoDbFactory));
		Document basicDBObject = documentConverter.convertToBsonDocument(spaceObject);
		Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
		Document recreatedBasicDbObject = documentConverter.convertToBsonDocument(reCreated);
		assertNotNull(recreatedBasicDbObject.get("_id"), "No id field defined. @SpaceId annotations are ignored by persistence framework, use @Id for id field (Typically the same as is annotated with @SpaceId)");
		Assertions.assertEquals(basicDBObject.get("_id"), recreatedBasicDbObject.get("_id"));
	}

	@ParameterizedTest
	@MethodSource("testCases")
	void testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty(ConverterTest<?> testCase) throws Exception {
		Object spaceObject = testCase.spaceObject;
		Field[] fields = spaceObject.getClass().getDeclaredFields();
		List<String> emptyFields = new LinkedList<>();

		for (Field field : fields) {
			field.setAccessible(true);
			Object fieldValue = field.get(spaceObject);
			if (fieldValue instanceof Collection) {
				if (((Collection<?>) fieldValue).isEmpty()) {
					emptyFields.add(field.getName());
				}
			}
			if (fieldValue instanceof Map) {
				if (((Map<?, ?>) fieldValue).isEmpty()) {
					emptyFields.add(field.getName());
				}
			}
		}

		assertTrue(emptyFields.isEmpty(), "Test subject of class " + spaceObject.getClass().getCanonicalName() + " has empty collections/map, "
				+ "add at least one element to ensure proper test coverage.: " + emptyFields);
	}

	private MirroredObjectTestHelper getMirroredObjectHelper(Class<?> objectClass) {
		return MirroredObjectTestHelper.fromDefinitions(getMirroredObjectDefinitions(), objectClass);
	}

	protected abstract Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions();

	protected abstract MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory);

	protected abstract Collection<ConverterTest<?>> testCases();

	protected static class ConverterTest<T> {

		final T spaceObject;
		final Matcher<T> matcher;

		/**
		 * Creates a converter test that serializes and deserializes the given space-object.
		 * <p>
		 * Matching for determining if deserialized object is 'correct' will be made using
		 * Matchers.samePropertyValuesAs(spaceObject).
		 */
		public ConverterTest(T spaceObject) {
			this(spaceObject, Matchers.samePropertyValuesAs(spaceObject));
		}

		/**
		 * Creates a converter test that serializes and deserializes the given space-object.
		 * <p>
		 * Uses given matcher to check if deserialized version is correct.
		 */
		public ConverterTest(T spaceObject, Matcher<T> matcher) {
			this.spaceObject = spaceObject;
			this.matcher = matcher;
		}

		@Override
		public String toString() {
			return "ConverterTest: " + spaceObject.getClass();
		}
	}

}
