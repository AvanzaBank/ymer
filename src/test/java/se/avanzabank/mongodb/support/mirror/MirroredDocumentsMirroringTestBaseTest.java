package se.avanzabank.mongodb.support.mirror;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.gigaspaces.annotation.pojo.SpaceId;

import se.avanzabank.mongodb.support.mirror.MirroredDocumentsMirroringTestBase.MirrorTest;

public class MirroredDocumentsMirroringTestBaseTest {

	@Test
	public void testingIfMirroredObjectIsMirrroedPasses() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(new MirrorTest<>(new TestSpaceObject("foo", "message")));
		assertPasses(new TestRun() {
			void run() {
				test1.canMirrorSpaceObject();
			}
		});
	}

	@Test
	public void testingIfNonMirroredObjectIsMirroredFails() throws Exception {
		class NonMirroredType {

		}
		final FakeTestSuite test1 = new FakeTestSuite(new MirrorTest<>(new NonMirroredType()));
		assertFails(new TestRun() {
			void run() {
				test1.canMirrorSpaceObject();
			}
		});
	}

	@Test
	public void serializationTestSucceds() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(
				new MirrorTest<>(new TestSpaceObject("foo", "message"), new TypeSafeMatcher<TestSpaceObject>() {
					@Override
					public void describeTo(Description arg0) {
					}

					@Override
					protected boolean matchesSafely(TestSpaceObject item) {
						return true;
					}
				}));
		assertPasses(new TestRun() {
			void run() throws Exception {
				test1.serializationTest();
			}
		});
	}

	@Test
	public void serializationTestFails() throws Exception {
		final FakeTestSuite test1 = new FakeTestSuite(
				new MirrorTest<>(new TestSpaceObject("foo", "message"), new TypeSafeMatcher<TestSpaceObject>() {
					@Override
					public void describeTo(Description arg0) {
					}

					@Override
					protected boolean matchesSafely(TestSpaceObject item) {
						return false;
					}
				}));
		assertFails(new TestRun() {
			void run() throws Exception {
				test1.serializationTest();
			}
		});
	}

	@Test
	public void spaceObjectWithoutAnnotationTestFails() throws Exception {
		final FakeTestSuiteWithoutId test1 = new FakeTestSuiteWithoutId(
				new MirrorTest<>(new TestSpaceObjectWithoutSpringDataIdAnnotation("foo"),
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
			void run() throws Exception {
				test1.testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject();
			}
		});
	}

	@Test
	public void spaceObjectWithEmptyCollectionTestFails() throws Exception {
		final FakeTestSuiteWithEmptyCollection test = new FakeTestSuiteWithEmptyCollection(
			new MirrorTest<>(new TestSpaceObjectWithEmptyCollection(),
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
			void run() throws Exception {
				test.testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty();
			}
		});
	}
	
	@Test
	public void spaceObjectWithEmptyMapTestFails() throws Exception {
		final FakeTestSuiteWithEmptyMap test = new FakeTestSuiteWithEmptyMap(
			new MirrorTest<>(new TestSpaceObjectWithEmptyMap(),
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
			void run() throws Exception {
				test.testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty();
			}
		});
	}

	class FakeTestSuiteWithEmptyCollection extends MirroredDocumentsMirroringTestBase {

		public FakeTestSuiteWithEmptyCollection(MirrorTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected MirroredDocuments getMirroredDocuments() {
			return new MirroredDocuments(new MirroredDocument<>(TestSpaceObjectWithEmptyCollection.class));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		}

	}
	
	class FakeTestSuiteWithEmptyMap extends MirroredDocumentsMirroringTestBase {

		public FakeTestSuiteWithEmptyMap(MirrorTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected MirroredDocuments getMirroredDocuments() {
			return new MirroredDocuments(new MirroredDocument<>(TestSpaceObjectWithEmptyMap.class));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		}

	}

	class FakeTestSuite extends MirroredDocumentsMirroringTestBase {

		public FakeTestSuite(MirrorTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected MirroredDocuments getMirroredDocuments() {
			return new MirroredDocuments(new MirroredDocument<>(TestSpaceObject.class));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		}

	}

	class FakeTestSuiteWithoutId extends MirroredDocumentsMirroringTestBase {

		public FakeTestSuiteWithoutId(MirrorTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected MirroredDocuments getMirroredDocuments() {
			return new MirroredDocuments(new MirroredDocument<>(TestSpaceObjectWithoutSpringDataIdAnnotation.class));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
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
