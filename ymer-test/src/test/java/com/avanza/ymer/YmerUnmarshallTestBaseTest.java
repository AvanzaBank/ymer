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

import com.avanza.ymer.YmerUnmarshallTestBase.UnmarshallTest;
import com.mongodb.BasicDBObject;
import org.junit.Test;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class YmerUnmarshallTestBaseTest {

	@Test
	public void testingIfMirroredObjectIsMirroredPasses() throws Exception {
		FakeTestSuite testSuite = new FakeTestSuite(new UnmarshallTest<>(new BasicDBObject(), new TestSpaceObject("foo", "message")));
		assertPasses(() -> testSuite.canMirrorSpaceObject());
	}

	@Test
	public void testingIfNonMirroredObjectIsMirroredFails() throws Exception {
		class NonMirroredType {

		}
		FakeTestSuite testSuite = new FakeTestSuite(new UnmarshallTest<>(new BasicDBObject(), new NonMirroredType()));
		assertFails(() -> testSuite.canMirrorSpaceObject());
	}

	@Test
	public void documentVersionIsCurrentVersionPasses() throws Exception {
		BasicDBObject document = new BasicDBObject();
		document.put("_formatVersion", 2);
		TestSpaceObject expectedSpaceObject = new TestSpaceObject("foo", "bar");

		FakeTestSuite testSuite = new FakeTestSuite(new UnmarshallTest<>(document, new TestSpaceObject("foo", "message")));
		assertPasses(() -> testSuite.documentVersionIsCurrentVersion());
	}

	@Test
	public void documentVersionIsCurrentVersionFails() throws Exception {
		BasicDBObject document = new BasicDBObject();
		document.put("_formatVersion", 3);
		TestSpaceObject expectedSpaceObject = new TestSpaceObject("foo", "bar");

		FakeTestSuite testSuite = new FakeTestSuite(new UnmarshallTest<>(document, new TestSpaceObject("foo", "message")));
		assertFails(() -> testSuite.documentVersionIsCurrentVersion());
	}

	@Test
	public void unmarshallCurrentVersionOfDocumentToSpaceObjectPasses() throws Exception {
		BasicDBObject document = new BasicDBObject();
		document.put("_id", "foo");
		document.put("message", "bar");
		TestSpaceObject expectedSpaceObject = new TestSpaceObject("foo", "bar");

		FakeTestSuite testSuite = new FakeTestSuite(new UnmarshallTest<>(document, expectedSpaceObject));
		assertPasses(() -> testSuite.unmarshallCurrentVersionOfDocumentToSpaceObject());
	}

	@Test
	public void unmarshallCurrentVersionOfDocumentToSpaceObjectFails() throws Exception {
		BasicDBObject document = new BasicDBObject();
		document.put("_id", "foo");
		document.put("message", null);
		TestSpaceObject expectedSpaceObject = new TestSpaceObject("foo", "bar");

		FakeTestSuite testSuite = new FakeTestSuite(new UnmarshallTest<>(document, expectedSpaceObject));
		assertFails(() -> testSuite.unmarshallCurrentVersionOfDocumentToSpaceObject());
	}

	class FakeTestSuite extends YmerUnmarshallTestBase {

		public FakeTestSuite(UnmarshallTest<?> testCase) {
			super(testCase);
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			DocumentPatch[] patches = { new DocumentPatch() {
				@Override
				public void apply(BasicDBObject dbObject) {
					dbObject.put("baz", "baz");
				}
				@Override
				public int patchedVersion() {
					return 1;
				}
			}};
			return Arrays.asList(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));
		}

		@Override
		protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
			return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
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
		} catch (AssertionError e) {
			fail("Expected test to pass");
		} catch (Exception e) {
			fail("Expected test to pass, but exception thrown");
		}
	}

	interface TestRun {

		void run() throws Exception;

	}

}
