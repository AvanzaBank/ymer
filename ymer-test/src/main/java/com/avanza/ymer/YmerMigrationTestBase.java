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

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.mongodb.BasicDBObject;

/**
 * Base class for testing that migration of documents is working properly.
 * 
 * Example usage: 
 * <pre>
 * public class AuthenticationMigrationTest extends YmerMigrationTestBase {
 * 
 * 	public AuthenticationMigrationTest(MigrationTest testCase) {
 *		super(testCase);
 *	}
 *
 *	&#64;Override
 *	protected MirroredObjects getMirroredObjectDefinitions() {
 *		return AuthenticationSpaceMirrorFactory.getMirroredObjectDefinitions();
 *	}
 *
 *	&#64;Parameters
 *	public static List&#60;Object[]&#62; testCases() {
 *		return buildTestCases(
 *			spaceActivationV1ToV2MigrationTest()
 *		);
 *	}
 *	
 *	private static MigrationTest spaceActivationV1ToV2MigrationTest() {
 *		BasicDBObject v1Doc = new BasicDBObject();
 *		v1Doc.put("_id", "un|foppa");
 *		v1Doc.put("activationCode", new BasicDBObject("code", 2142));
 *		v1Doc.put("to_be_removed", 2);
 *		
 *		BasicDBObject v2Doc = new BasicDBObject();
 *		v2Doc.put("_id", "un|foppa");
 *		v2Doc.put("activationCode", new BasicDBObject("code", 2142));
 *		
 *		return new MigrationTest(v1Doc, v2Doc, 1, SpaceActivation.class);
 *	}
 * }
 * </pre>
 * 
 *
 */
@RunWith(Parameterized.class)
public abstract class YmerMigrationTestBase {

	private MigrationTest migrationTest;

	public YmerMigrationTestBase(MigrationTest testCase) {
		this.migrationTest = testCase;
	}
	
	@Test
	public void migratesTheOldDocumentToTheNextDocumentVersion() throws Exception {
		getMirroredObjects().getMirroredObject(migrationTest.spaceObjectType).setDocumentVersion(migrationTest.toBePatched, migrationTest.fromVersion);
		
		MirroredObject<?> mirroredDocument = getMirroredObjects().getMirroredObject(migrationTest.spaceObjectType);
		
		Document patched = new Document(migrationTest.toBePatched);
		mirroredDocument.patchToNextVersion(patched);
		
		mirroredDocument.setDocumentVersion(migrationTest.expectedPatchedVersion, migrationTest.fromVersion + 1);
		
		assertEquals(migrationTest.expectedPatchedVersion, patched);
		assertEquals(migrationTest.fromVersion + 1, mirroredDocument.getDocumentVersion(patched));
	}
	
	@Test
	public void oldVersionShouldRequirePatching() {
		getMirroredObjects().getMirroredObject(migrationTest.spaceObjectType).setDocumentVersion(migrationTest.toBePatched, migrationTest.fromVersion);
		assertTrue("Should reqiure patching: " + migrationTest.toBePatched, getMirroredObjects().getMirroredObject(migrationTest.spaceObjectType).requiresPatching(migrationTest.toBePatched));
	}
	
	@Test
	public void targetSpaceTypeShouldBeAMirroredType() {
		assertTrue("Mirroring of " + migrationTest.getClass(), getMirroredObjects().isMirroredType(migrationTest.spaceObjectType));
	}
	
	private MirroredObjects getMirroredObjects() {
		return new MirroredObjects(getMirroredObjectDefinitions().stream(), MirroredObjectDefinitionsOverride.noOverride());
	}
	
	protected abstract Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions();
	
	
	protected final static List<Object[]> buildTestCases(MigrationTest... list) {
		List<Object[]> result = new ArrayList<Object[]>();
		for (MigrationTest testCase : list) {
			result.add(new Object[] { testCase });
		}
		return result;
	}

	protected static class MigrationTest {
		
		final Document toBePatched;
		final Document expectedPatchedVersion;
		final int fromVersion;
		final Class<?> spaceObjectType;

		/**
		 * 
		 * @param oldVersionDoc The document to be patched (one step)t
		 * @param expectedPatchedVersion
		 * @param oldVersion
		 * @param spaceObjectType
		 */
		public MigrationTest(Document oldVersionDoc, Document expectedPatchedVersion, int oldVersion, Class<?> spaceObjectType) {
			this.toBePatched = oldVersionDoc;
			this.expectedPatchedVersion = expectedPatchedVersion;
			this.fromVersion = oldVersion;
			this.spaceObjectType = spaceObjectType;
		}

		/**
		 * @deprecated Please use {@link #MigrationTest(Document, Document, int, Class)} instead.
		 */
		@Deprecated
		public MigrationTest(BasicDBObject oldVersionDoc, BasicDBObject expectedPatchedVersion, int oldVersion, Class<?> spaceObjectType) {
			this(
					new Document(oldVersionDoc),
					new Document(expectedPatchedVersion),
					oldVersion,
					spaceObjectType
			);
		}
	}
}
