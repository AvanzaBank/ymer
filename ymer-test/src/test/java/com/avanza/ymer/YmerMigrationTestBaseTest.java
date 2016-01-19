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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.avanza.ymer.DocumentPatch;
import com.avanza.ymer.MirroredDocument;
import com.avanza.ymer.MirroredDocuments;
import com.avanza.ymer.YmerMigrationTestBase.MigrationTest;
import com.mongodb.BasicDBObject;


public class YmerMigrationTestBaseTest {
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion_OnePatch_PassesIfNextVersionMatchesExpectedVersion() throws Exception {
		BasicDBObject v1 = new BasicDBObject();
		v1.put("foo", "bar");
		
		BasicDBObject v2 = new BasicDBObject();
		v2.put("foo", "bar");
		v2.put("baz", "baz");
		DocumentPatch[] patches = { new DocumentPatch() {
			@Override
			public void apply(BasicDBObject dbObject) {
				dbObject.put("baz", "baz");
			}
			@Override
			public int patchedVersion() {
				return 1;
			}
		} };
		
		final MirroredDocuments mirroredDocuments = new MirroredDocuments(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches).buildMirroredDocument());
		
		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertPasses(new TestRun() {
			@Override
			void run() throws Exception {
				new FakeTestSuite(testCase, mirroredDocuments).migratesTheOldDocumentToTheNextDocumentVersion();
			}
		});
	}
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion_OnePatch_FailsIfNextVersionDoesNotMatchExpectedVersion() throws Exception {
		BasicDBObject v1 = new BasicDBObject();
		v1.put("foo", "bar");
		
		BasicDBObject v2 = new BasicDBObject();
		v2.put("foo", "bar");
		v2.put("baz", "baz<");
		DocumentPatch[] patches = { new DocumentPatch() {
			@Override
			public void apply(BasicDBObject dbObject) {
				dbObject.put("baz", "baz");
			}
			@Override
			public int patchedVersion() {
				return 1;
			}
		} };
		
		final MirroredDocuments mirroredDocuments = new MirroredDocuments(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches).buildMirroredDocument());
		
		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertFails(new TestRun() {
			@Override
			void run() throws Exception {
				new FakeTestSuite(testCase, mirroredDocuments).migratesTheOldDocumentToTheNextDocumentVersion();
			}
		});
	}
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion_TwoPatch_PassesIfDocumentMatchesTheNextVersion() throws Exception {
		BasicDBObject v1 = new BasicDBObject();
		v1.put("foo", "foo");
		
		BasicDBObject v2 = new BasicDBObject();
		v2.put("foo", "foo");
		v2.put("bar", "bar");
		
		BasicDBObject v3 = new BasicDBObject();
		v3.put("foo", "bar");
		v3.put("bar", "bar");
		v3.put("baz", "baz");
		DocumentPatch[] patches = { new DocumentPatch() {
			@Override
			public void apply(BasicDBObject dbObject) {
				dbObject.put("bar", "bar");
			}
			@Override
			public int patchedVersion() {
				return 1;
			}
		}, new DocumentPatch() {
			@Override
			public void apply(BasicDBObject dbObject) {
				dbObject.put("baz", "baz");
			}
			@Override
			public int patchedVersion() {
				return 2;
			}
		} };
		
		final MirroredDocuments mirroredDocuments = new MirroredDocuments(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches).buildMirroredDocument());
		
		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertPasses(new TestRun() {
			@Override
			void run() throws Exception {
				new FakeTestSuite(testCase, mirroredDocuments).migratesTheOldDocumentToTheNextDocumentVersion();
			}
		});
	}

	class FakeTestSuite extends YmerMigrationTestBase {

		private final MirroredDocuments mirroredDocuments;

		public FakeTestSuite(MigrationTest testCase, MirroredDocuments mirroredDocuments) {
			super(testCase);
			this.mirroredDocuments = mirroredDocuments;
		}

		@Override
		protected MirroredDocuments getMirroredDocuments() {
			return mirroredDocuments;
		}

	}
	
	
	private void assertFails(TestRun testRun) {
		boolean failed = false;
		try {
			testRun.run();
		} catch (AssertionError e) {
			failed = true;
		} catch (Exception e) {
			failed = true;
		}
		assertTrue("Expected test to fail", failed);
	}

	private void assertPasses(TestRun testRun) {
		try {
			testRun.run();
			return;
		} catch (AssertionError e) {
			fail("Expected test to pass, failed with: " + e.getMessage());
		} catch (Exception e) {
			fail("Expected test to pass, but exception thrown");
		}
	}



	public static abstract class TestRun {
		
		abstract void run() throws Exception;
		
	}

}
