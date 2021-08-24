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

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.avanza.ymer.YmerConverterTestBase.ConverterTest;
import com.gigaspaces.annotation.pojo.SpaceId;

public class YmerConverterTestBaseTest {

	@Test
	public void testingIfMirroredObjectIsMirroredPasses() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(new ConverterTest<>(new TestSpaceObject("foo", "message")));
		assertPasses(test1::canMirrorSpaceObject);
	}

	@Test
	public void testingIfNonMirroredObjectIsMirroredFails() throws Exception {
		class NonMirroredType {

		}
		final FakeTestSuite test1 = new FakeTestSuite(new ConverterTest<>(new NonMirroredType()));
		assertFails(test1::canMirrorSpaceObject);
	}

	@Test
	public void serializationTestSucceeds() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(
				new ConverterTest<>(new TestSpaceObject("foo", "message"), anything()));
		assertPasses(test1::serializationTest);
	}

	@Test
	public void serializationTestFails() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(
				new ConverterTest<>(new TestSpaceObject("foo", "message"), not(anything())));
		assertFails(test1::serializationTest);
	}

	@Test
	public void spaceObjectWithoutIdAnnotationTestFails() throws Exception {
		final FakeTestSuiteWithoutId test1 = new FakeTestSuiteWithoutId(
				new ConverterTest<>(new TestSpaceObjectWithoutSpringDataIdAnnotation("foo"), anything()));
		assertFails(test1::testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject);
	}

	@Test
	public void spaceObjectWithEmptyCollectionTestFails() throws Exception {
		final FakeTestSuiteWithEmptyCollection test = new FakeTestSuiteWithEmptyCollection(
				new ConverterTest<>(new TestSpaceObjectWithEmptyCollection(), anything()));

		assertFails(test::testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty);
	}

	@Test
	public void spaceObjectWithEmptyMapTestFails() throws Exception {
		final FakeTestSuiteWithEmptyMap test = new FakeTestSuiteWithEmptyMap(
				new ConverterTest<>(new TestSpaceObjectWithEmptyMap(), anything()));

		assertFails(test::testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty);
	}

	static class FakeTestSuiteWithEmptyCollection extends YmerConverterTestBase {

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

	static class FakeTestSuiteWithEmptyMap extends YmerConverterTestBase {

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

	static class FakeTestSuite extends YmerConverterTestBase {

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

	static class FakeTestSuiteWithoutId extends YmerConverterTestBase {

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

	public static class TestSpaceObjectWithEmptyCollection {
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

	public static class TestSpaceObjectWithEmptyMap {
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

	public static class TestSpaceObjectWithoutSpringDataIdAnnotation {

		public TestSpaceObjectWithoutSpringDataIdAnnotation() {
		}

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

	private static void assertFails(ThrowingRunnable testRun) {
		Throwable exception = assertThrows("Expected test to fail", Throwable.class, testRun);

		if(!(exception instanceof AssertionError)) {
			exception.printStackTrace();
		}
	}

	private static void assertPasses(ThrowingRunnable testRun) {
		try {
			testRun.run();
		} catch (AssertionError e) {
			fail("Expected test to pass");
		} catch (Throwable e) {
			fail("Expected test to pass, but exception thrown");
		}
	}

}
