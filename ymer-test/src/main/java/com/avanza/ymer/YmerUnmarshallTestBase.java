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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Base class for testing that the current format version of a document
 * can be unmarshalled into an object. <p>
 */
@RunWith(Parameterized.class)
public abstract class YmerUnmarshallTestBase {

	private UnmarshallTest testCase;
	private MongoDbFactory dummyMongoDbFactory;

	public YmerUnmarshallTestBase(UnmarshallTest testCase) {
		this.testCase = testCase;
		this.dummyMongoDbFactory = new MongoDbFactory() {
			// The MongoDbFactory is never used during the tests.
			@Override
			public DB getDb(String dbName) throws DataAccessException {
				return null;
			}

			@Override
			public DB getDb() throws DataAccessException {
				return null;
			}

			@Override
			public PersistenceExceptionTranslator getExceptionTranslator() {
				return null;
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Test
	public void unmarshallCurrentVersionOfDocumentToSpaceObject() throws Exception {
		Object spaceObject = testCase.spaceObject;
		MirroredObject<?> mirroredDocument = getMirroredObjects().getMirroredObject(spaceObject.getClass());
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(createMongoConverter(dummyMongoDbFactory));
		Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), testCase.document);
		assertThat(reCreated, testCase.matcher);
	}

	@Test
	public void documentVersionIsCurrentVersion() throws Exception {
		Object spaceObject = testCase.spaceObject;
		MirroredObject<?> mirroredDocument = getMirroredObjects().getMirroredObject(spaceObject.getClass());
		int expectedDocumentVersion = mirroredDocument.getCurrentVersion();
		int actualDocumentVersion = testCase.document.getInt("_formatVersion", 1);
		Assert.assertEquals("Expected a document with current _formatVersion", expectedDocumentVersion, actualDocumentVersion);
	}

	@Test
	public void canMirrorSpaceObject() {
		assertTrue("Mirroring of " + testCase.getClass(), getMirroredObjects().isMirroredType(testCase.spaceObject.getClass()));
	}

	private MirroredObjects getMirroredObjects() {
		return new MirroredObjects(getMirroredObjectDefinitions().stream());
	}

	protected abstract Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions();

	protected abstract MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory);

	protected static List<Object[]> buildTestCases(UnmarshallTest... list) {
		List<Object[]> result = new ArrayList<>();
		for (UnmarshallTest testCase : list) {
			result.add(new Object[] { testCase });
		}
		return result;
	}

	protected static class UnmarshallTest<T> {

		final BasicDBObject document;
		final T spaceObject;
		final Matcher<T> matcher;

		/**
		 * Creates a test that unmarshalls a given document into an expected space-object.
		 *
		 * Matching for determining if unmarshalled object is 'correct' will be made using
		 * Matchers.samePropertyValuesAs(expectedSpaceObject).
		 *
		 * @param document
		 * @param expectedSpaceObject
		 */
		public UnmarshallTest(BasicDBObject document, T expectedSpaceObject) {
			this(document, expectedSpaceObject, Matchers.samePropertyValuesAs(expectedSpaceObject));
		}

		/**
		 * Creates a test that unmarshalls a given document into an expected space-object.
		 *
		 * Uses given matcher to check if unmarshalled version is correct. <p>
		 *
		 * @param document
		 * @param expectedSpaceObject
		 * @param matcher
		 */
		public UnmarshallTest(BasicDBObject document, T expectedSpaceObject, Matcher<T> matcher) {
			this.document = document;
			this.spaceObject = expectedSpaceObject;
			this.matcher = matcher;
		}
	}

}
