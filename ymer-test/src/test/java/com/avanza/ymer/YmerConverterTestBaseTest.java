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

import com.avanza.ymer.YmerConverterTestBase.ConverterTest;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class YmerConverterTestBaseTest {

	@Test
	public void testingIfMirroredObjectIsMirrroedPasses() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(new ConverterTest<>(new TestSpaceObject("foo", "message")));
		assertPasses(new TestRun() {
			@Override
			void run() {
				test1.canMirrorSpaceObject();
			}
		});
	}

	@Test
	public void testingIfNonMirroredObjectIsMirroredFails() throws Exception {
		class NonMirroredType {

		}
		final FakeTestSuite test1 = new FakeTestSuite(new ConverterTest<>(new NonMirroredType()));
		assertFails(new TestRun() {
			@Override
			void run() {
				test1.canMirrorSpaceObject();
			}
		});
	}

	@Test
	public void serializationTestSucceds() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(
				new ConverterTest<>(new TestSpaceObject("foo", "message"), new TypeSafeMatcher<TestSpaceObject>() {
					@Override
					public void describeTo(Description arg0) {
					}

					@Override
					protected boolean matchesSafely(TestSpaceObject item) {
						return true;
					}
				}));
		assertPasses(new TestRun() {
			@Override
			void run() throws Exception {
				test1.serializationTest();
			}
		});
	}

	@Test
	public void serializationTestFails() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(
				new ConverterTest<>(new TestSpaceObject("foo", "message"), new TypeSafeMatcher<TestSpaceObject>() {
					@Override
					public void describeTo(Description arg0) {
					}

					@Override
					protected boolean matchesSafely(TestSpaceObject item) {
						return false;
					}
				}));
		assertFails(new TestRun() {
			@Override
			void run() throws Exception {
				test1.serializationTest();
			}
		});
	}

	@Test
	public void spaceObjectWithoutIdAnnotationTestFails() throws Exception {
		final FakeTestSuiteWithoutId test1 = new FakeTestSuiteWithoutId(
				new ConverterTest<>(new TestSpaceObjectWithoutSpringDataIdAnnotation("foo"),
						new TypeSafeMatcher<TestSpaceObjectWithoutSpringDataIdAnnotation>() {
							@Override
							public void describeTo(Description arg0) {
							}

							@Override
							protected boolean matchesSafely(TestSpaceObjectWithoutSpringDataIdAnnotation item) {
								return true;
							}
						}));
		assertFails(new TestRun() {
			@Override
			void run() throws Exception {
				test1.testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject();
			}
		});
	}

	@Test
	public void spaceObjectWithEmptyCollectionTestFails() throws Exception {
		final FakeTestSuiteWithEmptyCollection test = new FakeTestSuiteWithEmptyCollection(
			new ConverterTest<>(new TestSpaceObjectWithEmptyCollection(),
				new TypeSafeMatcher<TestSpaceObjectWithEmptyCollection>() {
					@Override
					public void describeTo(Description arg0) {
					}

					@Override
					protected boolean matchesSafely(TestSpaceObjectWithEmptyCollection item) {
						return true;
					}
				}));

		assertFails(new TestRun() {
			@Override
			void run() throws Exception {
				test.testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty();
			}
		});
	}

	@Test
	public void spaceObjectWithEmptyMapTestFails() throws Exception {
		final FakeTestSuiteWithEmptyMap test = new FakeTestSuiteWithEmptyMap(
			new ConverterTest<>(new TestSpaceObjectWithEmptyMap(),
				new TypeSafeMatcher<TestSpaceObjectWithEmptyMap>() {
					@Override
					public void describeTo(Description arg0) {
					}

					@Override
					protected boolean matchesSafely(TestSpaceObjectWithEmptyMap item) {
						return true;
					}
				}));

		assertFails(new TestRun() {
			@Override
			void run() throws Exception {
				test.testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty();
			}
		});
	}

	class FakeTestSuiteWithEmptyCollection extends YmerConverterTestBase {

		public FakeTestSuiteWithEmptyCollection(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return Arrays.asList(MirroredObjectDefinition.create(TestSpaceObjectWithEmptyCollection.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	class FakeTestSuiteWithEmptyMap extends YmerConverterTestBase {

		public FakeTestSuiteWithEmptyMap(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return Arrays.asList(MirroredObjectDefinition.create(TestSpaceObjectWithEmptyMap.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	class FakeTestSuite extends YmerConverterTestBase {

		public FakeTestSuite(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return Arrays.asList(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	class FakeTestSuiteWithoutId extends YmerConverterTestBase {

		public FakeTestSuiteWithoutId(ConverterTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = {};
			return Arrays.asList(MirroredObjectDefinition.create(TestSpaceObjectWithoutSpringDataIdAnnotation.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
		}

	}

	public static class TestSpaceObjectWithEmptyCollection {
		@Id
		String id;

		Collection<String> emptyCollection = Collections.<String>emptyList();

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

		Map<String, String> map = Collections.<String, String>emptyMap();

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

	public static class TestSpaceObjectWithAutoGeneratedIdAndWithoutRoutingKeyAnnotation {
		private String id;

		public TestSpaceObjectWithAutoGeneratedIdAndWithoutRoutingKeyAnnotation() {
		}

		public TestSpaceObjectWithAutoGeneratedIdAndWithoutRoutingKeyAnnotation(String id) {
			this.id = id;
		}

		@SpaceId(autoGenerate = true)
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

	public static class WithoutAutoGeneratedIdOrRoutingAnnotation {
		private String id;

		public WithoutAutoGeneratedIdOrRoutingAnnotation() {
		}

		public WithoutAutoGeneratedIdOrRoutingAnnotation(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

	public static class TestSpaceObjectWithAutoGeneratedIdAndRoutingKeyAnnotation {
		private String id;

		public TestSpaceObjectWithAutoGeneratedIdAndRoutingKeyAnnotation() {
		}

		public TestSpaceObjectWithAutoGeneratedIdAndRoutingKeyAnnotation(String id) {
			this.id = id;
		}

		@SpaceRouting
		@SpaceId(autoGenerate = true)
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

	public static class TestSpaceObjectInheritingAutoGeneratedIdAndRoutingKeyAnnotation
			extends TestSpaceObjectWithAutoGeneratedIdAndRoutingKeyAnnotation {
	}

	public static class TestSpaceObjectWithRoutingKeyAnnotationOnPrivateProperty {
		private String id;
		private String routingKey;

		public TestSpaceObjectWithRoutingKeyAnnotationOnPrivateProperty() {
		}

		public TestSpaceObjectWithRoutingKeyAnnotationOnPrivateProperty(String id) {
			this.id = id;
		}

		@SpaceRouting
		private String getRoutingKey() {
			return routingKey;
		}

		@SpaceId(autoGenerate = true)
		private String getId() {
			return id;
		}

	}

	private void assertFails(TestRun testRun) {
		boolean failed = false;
		try {
			testRun.run();
		} catch (AssertionError e) {
			failed = true;
		} catch (Exception e) {
			e.printStackTrace();
			failed = true;
		}
		assertTrue("Expected test to fail", failed);
	}

	private void assertPasses(TestRun testRun) {
		try {
			testRun.run();
			return;
		} catch (AssertionError e) {
			fail("Expected test to pass");
		} catch (Exception e) {
			fail("Expected test to pass, but exception thrown");
		}
	}

	public static abstract class TestRun {

		abstract void run() throws Exception;

	}

}
