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

import static com.avanza.ymer.test.SpaceClassTestHelper.ensureSpaceId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.assertj.core.matcher.AssertionMatcher;
import org.bson.Document;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;

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

	public YmerConverterTestBase(ConverterTest<?> testCase) {
		this.testCase = testCase;
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

	@Test
	public void shouldRequireAnnotationsOnCustomConverters() {
		for (Converter<?, ?> customConverter : getMirroredObjectsConfiguration().getCustomConverters()) {
			boolean hasReadingConverterAnnotation = customConverter.getClass().getAnnotation(ReadingConverter.class) != null;
			boolean hasWritingConverterAnnotation = customConverter.getClass().getAnnotation(WritingConverter.class) != null;
			if (!hasReadingConverterAnnotation && !hasWritingConverterAnnotation) {
				fail("Custom converter=[" + customConverter.getClass() + "] should be annotated with either @ReadingConverter or @WritingConverter");
			}
		}
	}

	private MirroredObjectTestHelper getMirroredObjectHelper(Class<?> objectClass) {
		return MirroredObjectTestHelper.fromDefinitions(getMirroredObjectDefinitions(), objectClass);
	}

	protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return getMirroredObjectsConfiguration().getMirroredObjectDefinitions();
	}

	protected CustomConversions getCustomConversions() {
		return new MongoCustomConversions(
				getMirroredObjectsConfiguration().getCustomConverters()
		);
	}

	protected abstract MirroredObjectsConfiguration getMirroredObjectsConfiguration();

	private MongoConverter createMongoConverter() {
		return YmerFactory.createMongoConverter(
				NoOpDbRefResolver.INSTANCE,
				getCustomConversions(),
				Optional.empty());
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
