package se.avanzabank.mongodb.support.mirror;

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
import org.springframework.data.annotation.Id;
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
public abstract class MirroredDocumentsMirroringTestBase {

	private MirrorTest testCase;
	private MongoDbFactory dummyMongoDbFactory;

	public MirroredDocumentsMirroringTestBase(MirrorTest<?> testCase) {
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
		};
	}
	
	@Test
	public void serializationTest() throws Exception {
		Object spaceObject = testCase.spaceObject;
		MirroredDocument<?> mirroredDocument = getMirroredDocuments().getMirroredDocument(spaceObject.getClass());
		
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(createMongoConverter(dummyMongoDbFactory));
		BasicDBObject basicDBObject = documentConverter.convertToDBObject(spaceObject);
		Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
		assertThat(reCreated, testCase.matcher);
	}
	
	@Test
	public void canMirrorSpaceObject() {
		assertTrue("Mirroring of " + testCase.getClass(), getMirroredDocuments().isMirroredType(testCase.spaceObject.getClass()));
	}

    @Test
    public void testFailsIfSpringDataIdAnnotationNotDefinedForSpaceObject() throws Exception{
        Object spaceObject = testCase.spaceObject;
        MirroredDocument<?> mirroredDocument = getMirroredDocuments().getMirroredDocument(spaceObject.getClass());

        DocumentConverter documentConverter = DocumentConverter.mongoConverter(createMongoConverter(dummyMongoDbFactory));
        BasicDBObject basicDBObject = documentConverter.convertToDBObject(spaceObject);
        Object reCreated = documentConverter.convert(mirroredDocument.getMirroredType(), basicDBObject);
        BasicDBObject recreatedBasicDbObject = documentConverter.convertToDBObject(reCreated);
        assertNotNull("No id field defined. @SpaceId annotations are ignored by persistence framework, use @Id for id field (Typically the same as is annotated with @SpaceId)", recreatedBasicDbObject.get("_id"));
        assertEquals("",basicDBObject.get("_id"), recreatedBasicDbObject.get("_id"));
    }
    
//    @Test
    public void testFailsIfSpaceClassIsMissingSpringIdAnnotation() throws Exception {
    	Object spaceObject = testCase.spaceObject;
    	Field[] fields = spaceObject.getClass().getDeclaredFields();
    	boolean hasAnnotation = false;
    	for (Field field : fields) {
			if (field.isAnnotationPresent(Id.class)){
				hasAnnotation = true;
			}
		}
    	assertTrue("@Id annotation is missing on class " + spaceObject.getClass().getCanonicalName() ,hasAnnotation);
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
	
	protected abstract MirroredDocuments getMirroredDocuments();
	
	protected abstract MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory);
	
	protected final static List<Object[]> buildTestCases(MirrorTest<?>... list) {
		List<Object[]> result = new ArrayList<Object[]>();
		for (MirrorTest<?> testCase : list) {
			result.add(new Object[] { testCase });
		}
		return result;
	}

	protected static class MirrorTest<T> {
		
		final T spaceObject;
		final Matcher<T> matcher;

		/**
		 * Creates a mirror tests that serializes and deserializes the given space-object.
		 * 
		 * Matching for determining if deserialized object is 'correct' will be made using
		 * Matchers.samePropertyValuesAs(spaceObject).  
		 * 
		 * @param spaceObject
		 */
		public MirrorTest(T spaceObject) {
			this(spaceObject, Matchers.samePropertyValuesAs(spaceObject));
		}

		/**
		 * Creates a mirror tests that serializes and deserializes the given space-object.
		 * 
		 * Uses given matcher to check if desirialized version is correct. <p>
		 * 
		 * @param spaceObject
		 * @param matcher
		 */
		public MirrorTest(T spaceObject, Matcher<T> matcher) {
			this.spaceObject = spaceObject;
			this.matcher = matcher;
		}
		
	}

}
