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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

/**
 * Base class for testing that objects may be marshalled to a mongo document and
 * then unmarshalled back into an object. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 */
@RunWith(Parameterized.class)
public abstract class YmerConverterTestBase {

	@SuppressWarnings("rawtypes")
	private ConverterTest testCase;
	private MongoDbFactory dummyMongoDbFactory;

	public YmerConverterTestBase(ConverterTest<?> testCase) {
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
	public void serializationTest() throws Exception {
		Object spaceObject = testCase.spaceObject;
		MirroredObject<?> mirroredDocument = getMirroredObjects().getMirroredObject(spaceObject.getClass());
		
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(createMongoConverter(dummyMongoDbFactory));
		BasicDBObject basicDBObject = documentConverter.convertToDBObject(spaceObject);
		Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
		assertThat(reCreated, testCase.matcher);
	}
	
	@Test
	public void canMirrorSpaceObject() {
		assertTrue("Mirroring of " + testCase.getClass(), getMirroredObjects().isMirroredType(testCase.spaceObject.getClass()));
	}

    @Test
    public void testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject() throws Exception{
        Object spaceObject = testCase.spaceObject;
        MirroredObject<?> mirroredDocument = getMirroredObjects().getMirroredObject(spaceObject.getClass());

        DocumentConverter documentConverter = DocumentConverter.mongoConverter(createMongoConverter(dummyMongoDbFactory));
        BasicDBObject basicDBObject = documentConverter.convertToDBObject(spaceObject);
        Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
        BasicDBObject recreatedBasicDbObject = documentConverter.convertToDBObject(reCreated);
        assertNotNull("No id field defined. @SpaceId annotations are ignored by persistence framework, use @Id for id field (Typically the same as is annotated with @SpaceId)", recreatedBasicDbObject.get("_id"));
        assertEquals("",basicDBObject.get("_id"), recreatedBasicDbObject.get("_id"));
    }
    
    @Test
    public void testFailsIfCollectionOrMapPropertyOfTestSubjectIsEmpty() throws Exception {
    	Object spaceObject = testCase.spaceObject;
    	Field[] fields = spaceObject.getClass().getDeclaredFields();
    	List<String> emptyFields = new LinkedList<>();
    	
    	for (Field field : fields) {
    		field.setAccessible(true);
			Object fieldValue = field.get(spaceObject);
			if (fieldValue instanceof Collection){
				if (((Collection<?>) fieldValue).isEmpty()){
					emptyFields.add(field.getName());
				}
			}
			if (fieldValue instanceof Map){
				if (((Map<?,?>) fieldValue).isEmpty()){
					emptyFields.add(field.getName());
				}
			}
		}
    	
    	assertTrue("Test subject of class " + spaceObject.getClass().getCanonicalName() + " has empty collections/map, "
    			+ "add at least one element to ensure proper test coverage.: " + emptyFields, emptyFields.isEmpty());
    }
	
	protected MirroredObjects getMirroredObjects() {
		return new MirroredObjects(getMirroredObjectDefinitions().stream());
	}
	
	protected abstract Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions();

	protected abstract MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory);
	
	protected final static List<Object[]> buildTestCases(ConverterTest<?>... list) {
		List<Object[]> result = new ArrayList<Object[]>();
		for (ConverterTest<?> testCase : list) {
			result.add(new Object[] { testCase });
		}
		return result;
	}

	protected static class ConverterTest<T> {
		
		final T spaceObject;
		final Matcher<T> matcher;

		/**
		 * Creates a converter test that serializes and deserializes the given space-object.
		 * 
		 * Matching for determining if deserialized object is 'correct' will be made using
		 * Matchers.samePropertyValuesAs(spaceObject).  
		 * 
		 * @param spaceObject
		 */
		public ConverterTest(T spaceObject) {
			this(spaceObject, Matchers.samePropertyValuesAs(spaceObject));
		}

		/**
		 * Creates a converter test that serializes and deserializes the given space-object.
		 * 
		 * Uses given matcher to check if deserialized version is correct. <p>
		 * 
		 * @param spaceObject
		 * @param matcher
		 */
		public ConverterTest(T spaceObject, Matcher<T> matcher) {
			this.spaceObject = spaceObject;
			this.matcher = matcher;
		}
		
	}

}
