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
package com.avanza.ymer.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;

import org.bson.Document;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.MirroredObjectTestHelper;

/**
 * Base class for testing that migration of documents is working properly.
 * <p>
 * Example usage:
 * <pre>
 * class AuthenticationMigrationTest extends YmerMigrationTestBase {
 *
 * 	&#64;Override
 * 	protected MirroredObjects getMirroredObjectDefinitions() {
 * 		return AuthenticationSpaceMirrorFactory.getMirroredObjectDefinitions();
 *    }
 *
 * 	&#64;Override
 * 	protected Collection&#60;MigrationTest&#62; testCases() {
 * 		return List.of(
 * 			spaceActivationV1ToV2MigrationTest()
 * 		);
 *    }
 *
 * 	private MigrationTest spaceActivationV1ToV2MigrationTest() {
 * 		BasicDBObject v1Doc = new BasicDBObject();
 * 		v1Doc.put("_id", "un|foppa");
 * 		v1Doc.put("activationCode", new BasicDBObject("code", 2142));
 * 		v1Doc.put("to_be_removed", 2);
 *
 * 		BasicDBObject v2Doc = new BasicDBObject();
 * 		v2Doc.put("_id", "un|foppa");
 * 		v2Doc.put("activationCode", new BasicDBObject("code", 2142));
 *
 * 		return new MigrationTest(v1Doc, v2Doc, 1, SpaceActivation.class);
 *    }
 * }
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class YmerMigrationTestBase {

	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("testCases")
	void migratesTheOldDocumentToTheNextDocumentVersion(MigrationTest migrationTest) {
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectsHelper(migrationTest.spaceObjectType);

		mirroredDocument.setDocumentVersion(migrationTest.toBePatched, migrationTest.fromVersion);

		Document patched = new Document(migrationTest.toBePatched);
		mirroredDocument.patchToNextVersion(patched);

		mirroredDocument.setDocumentVersion(migrationTest.expectedPatchedVersion, migrationTest.fromVersion + 1);

		assertEquals(migrationTest.expectedPatchedVersion, patched);
		assertEquals(migrationTest.fromVersion + 1, mirroredDocument.getDocumentVersion(patched));
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("testCases")
	void oldVersionShouldRequirePatching(MigrationTest migrationTest) {
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectsHelper(migrationTest.spaceObjectType);
		mirroredDocument.setDocumentVersion(migrationTest.toBePatched, migrationTest.fromVersion);
		assertTrue(mirroredDocument.requiresPatching(migrationTest.toBePatched), "Should require patching: " + migrationTest.toBePatched);
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@MethodSource("testCases")
	void targetSpaceTypeShouldBeAMirroredType(MigrationTest migrationTest) {
		MirroredObjectTestHelper mirroredDocument = getMirroredObjectsHelper(migrationTest.spaceObjectType);
		assertTrue(mirroredDocument.isMirroredType(), "Mirroring of " + migrationTest.getClass());
	}

	private MirroredObjectTestHelper getMirroredObjectsHelper(Class<?> mirroredType) {
		return MirroredObjectTestHelper.fromDefinitions(getMirroredObjectDefinitions(), mirroredType);
	}

	protected abstract Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions();

	protected abstract Collection<MigrationTest> testCases();

	protected static class MigrationTest {
   		final String displayName;
		final Document toBePatched;
		final Document expectedPatchedVersion;
		final int fromVersion;
		final Class<?> spaceObjectType;

		/**
		 * @param oldVersionDoc The document to be patched (one step)
		 */
		public MigrationTest(Document oldVersionDoc, Document expectedPatchedVersion, int oldVersion, Class<?> spaceObjectType) {
			this.toBePatched = oldVersionDoc;
			this.expectedPatchedVersion = expectedPatchedVersion;
			this.fromVersion = oldVersion;
			this.spaceObjectType = spaceObjectType;
			this.displayName = spaceObjectType.getCanonicalName();
		}

		public MigrationTest(String displayName, Document oldVersionDoc, Document expectedPatchedVersion, int oldVersion, Class<?> spaceObjectType) {
			this.toBePatched = oldVersionDoc;
			this.expectedPatchedVersion = expectedPatchedVersion;
			this.fromVersion = oldVersion;
			this.spaceObjectType = spaceObjectType;
			this.displayName = displayName;
		}

		@Override
		public String toString() {
			return "MigrationTest: " + spaceObjectType + ": " + displayName;
		}
	}
}
