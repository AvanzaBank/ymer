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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.assertj.core.matcher.AssertionMatcher;
import org.bson.Document;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.data.mongodb.core.convert.AbstractMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.ReflectionUtils;

import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * Base class for testing that objects may be marshalled to a mongo document and
 * then unmarshalled back into an object. <p>
 *
 * @author Elias Lindholm (elilin)
 */
@RunWith(Parameterized.class)
public abstract class YmerConverterTestBase {

	@SuppressWarnings("rawtypes")
	private final ConverterTest testCase;
	private final MongoDbFactory dummyMongoDbFactory;

	public YmerConverterTestBase(ConverterTest<?> testCase) {
		this.testCase = testCase;
		// The MongoDbFactory is never used during the tests.
		this.dummyMongoDbFactory = new SimpleMongoClientDbFactory("mongodb://xxx/unused");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void serializationTest() {
		Object spaceObject = testCase.spaceObject;
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectHelper(spaceObject.getClass());

		TestDocumentConverter documentConverter = TestDocumentConverter.create(createMongoConverter());
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
		TestDocumentConverter documentConverter = TestDocumentConverter.create(createMongoConverter());
		ensureSpaceId(spaceObject);

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

	private MongoConverter createMongoConverter() {
		MongoConverter converter = createMongoConverter(dummyMongoDbFactory);
		if (converter instanceof AbstractMongoConverter) {
			// In a real app, the instance of MongoConverter is usually
			// registered as a spring bean, which will make "afterPropertiesSet"
			// be called automatically. But here, we are not in a spring context
			// and therefore will need to call it manually since
			// MappingMongoConverter only updates its internal state of
			// "conversionService" once this is called.
			((AbstractMongoConverter) converter).afterPropertiesSet();
		}
		return converter;
	}

	private static void ensureSpaceId(Object spaceObject) {
		PropertyDescriptor spaceIdProperty = Arrays.stream(BeanUtils.getPropertyDescriptors(spaceObject.getClass()))
				.filter(it -> it.getReadMethod() != null)
				.filter(it -> it.getReadMethod().isAnnotationPresent(SpaceId.class))
				.findFirst()
				.orElseThrow(() -> new AssertionError("Could not find @SpaceId on " + spaceObject.getClass().getSimpleName()));

		Method readMethod = spaceIdProperty.getReadMethod();
		ReflectionUtils.makeAccessible(readMethod);
		if (readMethod.getAnnotation(SpaceId.class).autoGenerate() && ReflectionUtils.invokeMethod(readMethod, spaceObject) == null) {
			String uid = UUID.randomUUID().toString();
			Method writeMethod = spaceIdProperty.getWriteMethod();
			if (writeMethod != null) {
				ReflectionUtils.makeAccessible(writeMethod);
				ReflectionUtils.invokeMethod(writeMethod, spaceObject, uid);
				return;
			}
			Field field = ReflectionUtils.findField(spaceObject.getClass(), spaceIdProperty.getName());
			if (field != null) {
				ReflectionUtils.makeAccessible(field);
				ReflectionUtils.setField(field, spaceObject, uid);
				return;
			}
			throw new AssertionError("Could not set property " + spaceIdProperty.getName());
		}
	}

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
		 * <p>
		 * Matching for determining if deserialized object is 'correct' will be made using
		 * AssertJ recursive comparison equals.
		 */
		public ConverterTest(T spaceObject) {
			this(spaceObject, new AssertionMatcher<>() {
				@Override
				public void assertion(T deserializedObject) throws AssertionError {
					Assertions.assertThat(deserializedObject)
							.usingRecursiveComparison()
							.isEqualTo(spaceObject);
				}
			});
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

	}

}
