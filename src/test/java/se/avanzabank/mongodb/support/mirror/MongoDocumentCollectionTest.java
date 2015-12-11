package se.avanzabank.mongodb.support.mirror;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Query;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import se.avanzabank.mongodb.support.mirror.MirroredDocument.Flag;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MongoDocumentCollectionTest extends DocumentCollectionContract {

	private static final String DBNAME = "testdb";
	private static final String COLLECTION_NAME = "testcollection";
	
	Fongo mongoServer = new Fongo("mongo server 1");


	private DB db;
	private DBCollection mongoDbCollection;

	/* (non-Javadoc)
	 * @see se.avanzabank.mongodb.support.mirror.DocumentCollectionContract#createEmptyCollection()
	 */
	@Override
	protected DocumentCollection createEmptyCollection() {
		db = mongoServer.getDB(DBNAME);
		db.dropDatabase();
		mongoDbCollection = db.getCollection(COLLECTION_NAME);
		return new MongoDocumentCollection(mongoDbCollection);
	}

	@Test
	public void canLoadDocumentsRouted() throws Exception {
		MirroredDocument<FakeSpaceObject> mirroredDocument = MirroredDocument.createDocument(FakeSpaceObject.class, EnumSet.of(Flag.LoadDocumentsRouted));

		// Objects WITH routed field
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 1);
		doc1.put("value", "a");
		mirroredDocument.setDocumentAttributes(doc1, new FakeSpaceObject(1, "a"));

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 2);
		doc2.put("value", "b");
		mirroredDocument.setDocumentAttributes(doc2, new FakeSpaceObject(2, "b"));

		// Objects WITHOUT routed field
		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 3);
		doc3.put("value", "c");

		final BasicDBObject doc4 = new BasicDBObject();
		doc4.put("_id", 4);
		doc4.put("value", "d");

		MongoDocumentCollectionTest testCollection = new MongoDocumentCollectionTest();
		DocumentCollection documentCollection = testCollection.createEmptyCollection();
		documentCollection.insertAll(doc1, doc2, doc3, doc4);

		MirroredDocumentLoader<FakeSpaceObject> documentLoader = new MirroredDocumentLoader<>(
				documentCollection,
				FakeMirroredDocumentConverter.create(),
				mirroredDocument,
				SpaceObjectFilter.partitionFilter(mirroredDocument, 2, 2));
		documentLoader.loadAllObjects();

		List<FakeSpaceObject> loadedSpaceObjects = documentLoader.getLoadedSpaceObjects();
		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(1, "a")));
		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(3, "c")));
		assertEquals(2, loadedSpaceObjects.size());
	}

	@Test
	public void canLoadDocumentsRoutedWithoutWriteBack() throws Exception {
		MirroredDocument<FakeSpaceObject> mirroredDocument = MirroredDocument.createDocument(FakeSpaceObject.class, EnumSet.of(Flag.LoadDocumentsRouted, Flag.DoNotWriteBackPatchedDocuments));

		// Objects WITH routed field
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 1);
		doc1.put("value", "a");
		mirroredDocument.setDocumentAttributes(doc1, new FakeSpaceObject(1, "a"));

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 2);
		doc2.put("value", "b");
		mirroredDocument.setDocumentAttributes(doc2, new FakeSpaceObject(2, "b"));

		// Objects WITHOUT routed field
		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 3);
		doc3.put("value", "c");

		final BasicDBObject doc4 = new BasicDBObject();
		doc4.put("_id", 4);
		doc4.put("value", "d");

		MongoDocumentCollectionTest testCollection = new MongoDocumentCollectionTest();
		DocumentCollection documentCollection = testCollection.createEmptyCollection();
		documentCollection.insertAll(doc1, doc2, doc3, doc4);

		MirroredDocumentLoader<FakeSpaceObject> documentLoader = new MirroredDocumentLoader<>(
				documentCollection,
				FakeMirroredDocumentConverter.create(),
				mirroredDocument,
				SpaceObjectFilter.partitionFilter(mirroredDocument, 1, 2));
		documentLoader.loadAllObjects();

		List<FakeSpaceObject> loadedSpaceObjects = documentLoader.getLoadedSpaceObjects();
		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(2, "b")));
		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(4, "d")));
		assertEquals(2, loadedSpaceObjects.size());
	}

	private static class FakeSpaceObject {
		private final Integer id;
		private final String value;
		private FakeSpaceObject(Integer id, String value) {
			this.id = id;
			this.value = value;
		}
		@SpaceId(autoGenerate = true)
		public final Integer getId() {
			return id;
		}
		public final String getValue() {
			return value;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			MongoDocumentCollectionTest.FakeSpaceObject other = (MongoDocumentCollectionTest.FakeSpaceObject) obj;
			return getId().equals(other.getId())
					&& getValue().equals(other.getValue());
		}
		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}
		@Override
		public String toString() {
			return "FakeSpaceObject [id=" + id + ", value=" + value + "]";
		}
	}


	private static class FakeMirroredDocumentConverter implements DocumentConverter.Provider {
		@Override
		public <T> T convert(Class<T> toType, BasicDBObject document) {
			FakeSpaceObject spaceObject = new FakeSpaceObject(document.getInt("_id"), document.getString("value"));
			return toType.cast(spaceObject);
		}

		@Override
		public BasicDBObject convertToDBObject(Object type) {
			throw new UnsupportedOperationException();
		}

		public static DocumentConverter create() {
			return DocumentConverter.create(new FakeMirroredDocumentConverter());
		}

		@Override
		public Object convert(Object type) {
			if (type instanceof Number) {
				return type;
			}
			return type.toString();
		}

		@Override
		public Query toQuery(Object template) {
			throw new UnsupportedOperationException();
		}
	}

}
