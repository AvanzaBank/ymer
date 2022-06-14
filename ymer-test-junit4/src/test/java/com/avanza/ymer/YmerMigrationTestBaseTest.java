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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.avanza.ymer.YmerMigrationTestBase.MigrationTest;
import com.mongodb.BasicDBObject;

public class YmerMigrationTestBaseTest {
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion_OnePatch_PassesIfNextVersionMatchesExpectedVersion() throws Exception {
		Document v1 = new Document();
		v1.put("foo", "bar");

		Document v2 = new Document();
		v2.put("foo", "bar");
		v2.put("baz", "baz");
		BsonDocumentPatch[] patches = { new BsonDocumentPatch() {
			@Override
			public void apply(Document document) {
				document.put("baz", "baz");
			}

			@Override
			public int patchedVersion() {
				return 1;
			}
		} };
		
		final Collection<MirroredObjectDefinition<?>> mirroredObjects = List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));
		
		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertPasses(() -> new FakeTestSuite(testCase, mirroredObjects).migratesTheOldDocumentToTheNextDocumentVersion());
	}
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion_OnePatch_FailsIfNextVersionDoesNotMatchExpectedVersion() throws Exception {
		Document v1 = new Document();
		v1.put("foo", "bar");

		Document v2 = new Document();
		v2.put("foo", "bar");
		v2.put("baz", "baz<");
		BsonDocumentPatch[] patches = { new BsonDocumentPatch() {
			@Override
			public void apply(Document document) {
				document.put("baz", "baz");
			}

			@Override
			public int patchedVersion() {
				return 1;
			}
		} };

		final Collection<MirroredObjectDefinition<?>> mirroredObjects = List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));
		
		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertFails(() -> new FakeTestSuite(testCase, mirroredObjects).migratesTheOldDocumentToTheNextDocumentVersion());
	}
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion_TwoPatch_PassesIfDocumentMatchesTheNextVersion() throws Exception {
		Document v1 = new Document();
		v1.put("foo", "foo");

		Document v2 = new Document();
		v2.put("foo", "foo");
		v2.put("bar", "bar");

		Document v3 = new Document();
		v3.put("foo", "bar");
		v3.put("bar", "bar");
		v3.put("baz", "baz");
		BsonDocumentPatch[] patches = { new BsonDocumentPatch() {
			@Override
			public void apply(Document document) {
				document.put("bar", "bar");
			}

			@Override
			public int patchedVersion() {
				return 1;
			}
		}, new BsonDocumentPatch() {
			@Override
			public void apply(Document document) {
				document.put("baz", "baz");
			}

			@Override
			public int patchedVersion() {
				return 2;
			}
		} };

		final Collection<MirroredObjectDefinition<?>> mirroredObjects = List.of(MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(patches));
		
		final MigrationTest testCase = new MigrationTest(v1, v2, 1, TestSpaceObject.class);
		assertPasses(() -> new FakeTestSuite(testCase, mirroredObjects).migratesTheOldDocumentToTheNextDocumentVersion());
	}

	@Test
	public void alsoAcceptsDeprecatedBasicDbObjectInMigrationTest() throws Exception {
		// Arrange
		final BasicDBObject v1 = new BasicDBObject();
		v1.put("foo", "bar");

		final BasicDBObject expectedV2 = new BasicDBObject();
		expectedV2.put("foo", "bar");
		expectedV2.put("baz", "baz");

		final BsonDocumentPatch patch = new BsonDocumentPatch() {
			@Override
			public void apply(Document document) {
				document.put("baz", "baz");
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
		@SuppressWarnings("deprecation")
		var suite = new FakeTestSuite(
				new MigrationTest(v1, expectedV2, 1, TestSpaceObject.class),
				mirroredObjects);
		suite.migratesTheOldDocumentToTheNextDocumentVersion();

		// Assert that no exceptions were thrown
	}

	static class FakeTestSuite extends YmerMigrationTestBase {

		private final Collection<MirroredObjectDefinition<?>> mirroredObjects;

		public FakeTestSuite(MigrationTest testCase, Collection<MirroredObjectDefinition<?>> mirroredObjects) {
			super(testCase);
			this.mirroredObjects = mirroredObjects;
		}

		@Override
		protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			return mirroredObjects;
		}

	}
	
	
	private static void assertFails(ThrowingRunnable testRun) {
		assertThrows("Expected test to fail", Throwable.class, testRun);
	}

	private static void assertPasses(ThrowingRunnable testRun) {
		try {
			testRun.run();
		} catch (AssertionError e) {
			fail("Expected test to pass, failed with: " + e.getMessage());
		} catch (Throwable e) {
			fail("Expected test to pass, but exception thrown");
		}
	}

}
