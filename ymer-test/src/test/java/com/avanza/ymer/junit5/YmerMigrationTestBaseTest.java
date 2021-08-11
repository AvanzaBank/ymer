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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.avanza.ymer.DocumentPatch;
import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.TestSpaceObject;
import com.avanza.ymer.junit5.YmerMigrationTestBase.MigrationTest;
import com.mongodb.BasicDBObject;

class YmerMigrationTestBaseTest {

	@Test
	void migratesTheOldDocumentToTheNextDocumentVersion_OnePatch_PassesIfNextVersionMatchesExpectedVersion() {
		Document v1 = new Document();
		v1.put("foo", "bar");

		Document v2 = new Document();
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

		final Collection<MirroredObjectDefinition<?>> mirroredObjects = List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));

		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertPasses(() -> new FakeTestSuite(testCase, mirroredObjects).migratesTheOldDocumentToTheNextDocumentVersion(testCase));
	}

	@Test
	void migratesTheOldDocumentToTheNextDocumentVersion_OnePatch_FailsIfNextVersionDoesNotMatchExpectedVersion() {
		Document v1 = new Document();
		v1.put("foo", "bar");

		Document v2 = new Document();
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

		final Collection<MirroredObjectDefinition<?>> mirroredObjects = List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));

		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertFails(() -> new FakeTestSuite(testCase, mirroredObjects).migratesTheOldDocumentToTheNextDocumentVersion(testCase));
	}

	@Test
	void migratesTheOldDocumentToTheNextDocumentVersion_TwoPatch_PassesIfDocumentMatchesTheNextVersion() {
		Document v1 = new Document();
		v1.put("foo", "foo");

		Document v2 = new Document();
		v2.put("foo", "foo");
		v2.put("bar", "bar");

		Document v3 = new Document();
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

		final Collection<MirroredObjectDefinition<?>> mirroredObjects = List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));

		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertPasses(() -> new FakeTestSuite(testCase, mirroredObjects).migratesTheOldDocumentToTheNextDocumentVersion(testCase));
	}

	@Test
	void alsoAcceptsDeprecatedBasicDbObjectInMigrationTest() {
		// Arrange
		final Document v1 = new Document();
		v1.put("foo", "bar");

		final Document expectedV2 = new Document();
		expectedV2.put("foo", "bar");
		expectedV2.put("baz", "baz");

		final DocumentPatch patch = new DocumentPatch() {
			@Override
			public void apply(BasicDBObject dbObject) {
				dbObject.put("baz", "baz");
			}

			@Override
			public int patchedVersion() {
				return 1;
			}
		};
		final Collection<MirroredObjectDefinition<?>> mirroredObjects = Collections.singletonList(
				MirroredObjectDefinition.create(TestSpaceObject.class)
						.documentPatches(patch)
		);

		// Act
		var suite = new FakeTestSuite(
				new MigrationTest(v1, expectedV2, 1, TestSpaceObject.class),
				mirroredObjects);

		assertPasses(() -> suite.migratesTheOldDocumentToTheNextDocumentVersion(suite.testCase));
	}

	static class FakeTestSuite extends YmerMigrationTestBase {

		final MigrationTest testCase;
		final Collection<MirroredObjectDefinition<?>> mirroredObjects;

		FakeTestSuite(MigrationTest testCase, Collection<MirroredObjectDefinition<?>> mirroredObjects) {
			this.testCase = testCase;
			this.mirroredObjects = mirroredObjects;
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			return mirroredObjects;
		}

		@Override
		protected Collection<MigrationTest> testCases() {
			return List.of(testCase);
		}
	}

	private static void assertFails(Executable testRun) {
		assertThrows(AssertionError.class, testRun);
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
