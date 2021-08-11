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

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.avanza.ymer.DocumentPatch;
import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.TestSpaceObject;
import com.avanza.ymer.junit5.YmerConverterTestBase.ConverterTest;
import com.gigaspaces.annotation.pojo.SpaceId;

class YmerConverterTestBaseTest {

	@Test
	void testingIfMirroredObjectIsMirroredPasses() {
		final FakeTestSuite test1 = new FakeTestSuite(new ConverterTest<>(new TestSpaceObject("foo", "message")));
		assertPasses(() -> test1.canMirrorSpaceObject(test1.converterTest));
	}

	@Test
	void testingIfNonMirroredObjectIsMirroredFails() {
		class NonMirroredType {

		}
		final FakeTestSuite test1 = new FakeTestSuite(new ConverterTest<>(new NonMirroredType()));
		assertFails(() -> test1.canMirrorSpaceObject(test1.converterTest));
	}

	@Test
	void serializationTestSucceeds() {
		final FakeTestSuite test1 = new FakeTestSuite(
				new ConverterTest<>(new TestSpaceObject("foo", "message"), anything()));
		assertPasses(() -> test1.serializationTest(test1.converterTest));
	}

	@Test
	void serializationTestFails() {
		final FakeTestSuite test1 = new FakeTestSuite(
				new ConverterTest<>(new TestSpaceObject("foo", "message"), not(anything())));
		assertFails(() -> test1.serializationTest(test1.converterTest));
	}

	@Test
	void spaceObjectWithoutIdAnnotationTestFails() {
		final FakeTestSuiteWithoutId test1 = new FakeTestSuiteWithoutId(
				new ConverterTest<>(new TestSpaceObjectWithoutSpringDataIdAnnotation("foo"), anything()));
		assertFails(() -> test1.testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject(test1.converterTest));
	}

	@Test
	void spaceObjectWithEmptyCollectionTestFails() {
		final FakeTestSuiteWithEmptyCollection test = new FakeTestSuiteWithEmptyCollection(
				new ConverterTest<>(new TestSpaceObjectWithEmptyCollection(), anything()));

		assertFails(() -> test.testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject(test.converterTest));
	}

	@Test
	void spaceObjectWithEmptyMapTestFails() {
		final FakeTestSuiteWithEmptyMap test = new FakeTestSuiteWithEmptyMap(
				new ConverterTest<>(new TestSpaceObjectWithEmptyMap(), anything()));

		assertFails(() -> test.testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty(test.converterTest));
	}

	static abstract class FakeYmerConverterTestBase extends YmerConverterTestBase {
		final ConverterTest<?> converterTest;

		protected FakeYmerConverterTestBase(ConverterTest<?> converterTest) {
			this.converterTest = converterTest;
		}

		@Override
		protected Collection<ConverterTest<?>> testCases() {
			return List.of(converterTest);
		}
	}

	static class FakeTestSuiteWithEmptyCollection extends FakeYmerConverterTestBase {

		public FakeTestSuiteWithEmptyCollection(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return List.of(MirroredObjectDefinition.create(TestSpaceObjectWithEmptyCollection.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	static class FakeTestSuiteWithEmptyMap extends FakeYmerConverterTestBase {

		public FakeTestSuiteWithEmptyMap(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return List.of(MirroredObjectDefinition.create(TestSpaceObjectWithEmptyMap.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	static class FakeTestSuite extends FakeYmerConverterTestBase {

		public FakeTestSuite(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	static class FakeTestSuiteWithoutId extends FakeYmerConverterTestBase {

		public FakeTestSuiteWithoutId(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return List.of(MirroredObjectDefinition.create(TestSpaceObjectWithoutSpringDataIdAnnotation.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	static class TestSpaceObjectWithEmptyCollection {
		@Id
		String id;

		Collection<String> emptyCollection = Collections.emptyList();

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Collection<String> getEmptyCollection() {
			return emptyCollection;
		}

		public void setEmptyCollection(Collection<String> emptyCollection) {
			this.emptyCollection = emptyCollection;
		}

	}

	static class TestSpaceObjectWithEmptyMap {
		@Id
		String id;

		Map<String, String> map = Collections.emptyMap();

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public Map<String, String> getMap() {
			return map;
		}

		public void setMap(Map<String, String> emptyCollection) {
			this.map = emptyCollection;
		}

	}

	static class TestSpaceObjectWithoutSpringDataIdAnnotation {

		public TestSpaceObjectWithoutSpringDataIdAnnotation(String id) {
			this.identifier = id;
		}

		private String identifier;

		@SpaceId
		public String getIdentifier() {
			return identifier;
		}

		public void setIdentifier(String id) {
			this.identifier = id;
		}
	}

	private static void assertFails(Executable testRun) {
		assertThrows(Throwable.class, testRun);
	}

	private static void assertPasses(Executable testRun) {
		try {
			testRun.execute();
		} catch (AssertionError e) {
			fail("Expected test to pass, but failed with: " + e.getMessage());
		} catch (Throwable e) {
			fail("Unexpected exception in test: " + e.getMessage());
		}
	}

}
