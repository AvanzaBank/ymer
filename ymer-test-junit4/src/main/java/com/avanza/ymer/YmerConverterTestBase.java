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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.mongodb.DB;
import com.mongodb.client.MongoDatabase;

/**
 * Base class for testing that objects may be marshalled to a mongo document and
 * then unmarshalled back into an object. <p>
 *
 * @author Elias Lindholm (elilin)
 *
 */
@RunWith(Parameterized.class)
public abstract class YmerConverterTestBase {

	@SuppressWarnings("rawtypes")
	private final ConverterTest testCase;
	private final MongoDbFactory dummyMongoDbFactory;

	public YmerConverterTestBase(ConverterTest<?> testCase) {
		this.testCase = testCase;
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

	@SuppressWarnings("unchecked")
	@Test
	public void serializationTest() {
		Object spaceObject = testCase.spaceObject;
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(spaceObject.getClass());

		TestDocumentConverter documentConverter = TestDocumentConverter.create(createMongoConverter(dummyMongoDbFactory));
		Document basicDBObject = documentConverter.convertToBsonDocument(spaceObject);
		Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
		assertThat(reCreated, testCase.matcher);
	}

	@Test
	public void canMirrorSpaceObject() {
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(testCase.spaceObject.getClass());
		assertTrue("Mirroring of " + testCase.getClass(), mirroredDocument.isMirroredType());
	}

    @Test
    public void testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject() {
        Object spaceObject = testCase.spaceObject;
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(spaceObject.getClass());

		TestDocumentConverter documentConverter = TestDocumentConverter.create(createMongoConverter(dummyMongoDbFactory));
        Document basicDBObject = documentConverter.convertToBsonDocument(spaceObject);
        Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
        Document recreatedBasicDbObject = documentConverter.convertToBsonDocument(reCreated);
        assertNotNull("No id field defined. @SpaceId annotations are ignored by persistence framework, use @Id for id field (Typically the same as is annotated with @SpaceId)", recreatedBasicDbObject.get("_id"));
        assertEquals(basicDBObject.get("_id"), recreatedBasicDbObject.get("_id"));
    }

    @Test
    public void testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty() throws Exception {
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

    	assertTrue("Test subject of class " + spaceObject.getClass().getCanonicalName() + " has empty collections/map, "
    			+ "add at least one element to ensure proper test coverage.: " + emptyFields, emptyFields.isEmpty());
    }

	private MirroredObjectTestHelper getMirroredObjectHelper(Class<?> objectClass) {
		return MirroredObjectTestHelper.fromDefinitions(getMirroredObjectDefinitions(), objectClass);
	}

	protected abstract Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions();

	protected abstract MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory);

	protected static List<Object[]> buildTestCases(ConverterTest<?>... list) {
		List<Object[]> result = new ArrayList<>();
		for (ConverterTest<?> testCase : list) {
			result.add(new Object[] { testCase });
		}
		return result;
	}

	protected static class ConverterTest<T> {

		final T spaceObject;
		final Matcher<T> matcher;

		/**
		 * Creates a converter test that serializes and deserializes the given space-object.
		 *
		 * Matching for determining if deserialized object is 'correct' will be made using
		 * Matchers.samePropertyValuesAs(spaceObject).
		 *
		 */
		public ConverterTest(T spaceObject) {
			this(spaceObject, Matchers.samePropertyValuesAs(spaceObject));
		}

		/**
		 * Creates a converter test that serializes and deserializes the given space-object.
		 *
		 * Uses given matcher to check if deserialized version is correct. <p>
		 *
		 */
		public ConverterTest(T spaceObject, Matcher<T> matcher) {
			this.spaceObject = spaceObject;
			this.matcher = matcher;
		}

	}

}
