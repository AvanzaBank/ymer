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
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.Test;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.DocumentCollection;
import com.avanza.ymer.DocumentConverter;
import com.avanza.ymer.MirroredObject;
import com.avanza.ymer.MirroredObjectLoader;
import com.avanza.ymer.MongoDocumentCollection;
import com.avanza.ymer.SpaceObjectFilter;
import com.avanza.ymer.MirroredObject.Flag;
import com.avanza.ymer.MirroredObjectLoader.LoadedDocument;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

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
	 * @see com.avanza.ymer.DocumentCollectionContract#createEmptyCollection()
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
		DocumentPatch[] patches = {};
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).flags(Flag.LOAD_DOCUMENTS_ROUTED).documentPatches(patches).buildMirroredDocument();

		// Objects WITH routed field
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 1);
		doc1.put("value", "a");
		mirroredObject.setDocumentAttributes(doc1, new FakeSpaceObject(1, "a"));

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 2);
		doc2.put("value", "b");
		mirroredObject.setDocumentAttributes(doc2, new FakeSpaceObject(2, "b"));

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

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(
				documentCollection,
				FakeMirroredDocumentConverter.create(),
				mirroredObject,
				SpaceObjectFilter.partitionFilter(mirroredObject, 2, 2));
		
		List<FakeSpaceObject> loadedSpaceObjects = documentLoader.loadAllObjects()
					  .stream()
					  .map(LoadedDocument::getDocument)
					  .collect(Collectors.toList());
					 

		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(1, "a")));
		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(3, "c")));
		assertEquals(2, loadedSpaceObjects.size());
	}

	@Test
	public void canLoadDocumentsRoutedWithoutWriteBack() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).flags(Flag.LOAD_DOCUMENTS_ROUTED, Flag.DO_NOT_WRITE_BACK_PATCHED_DOCUMENTS).documentPatches(patches).buildMirroredDocument();

		// Objects WITH routed field
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 1);
		doc1.put("value", "a");
		mirroredObject.setDocumentAttributes(doc1, new FakeSpaceObject(1, "a"));

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 2);
		doc2.put("value", "b");
		mirroredObject.setDocumentAttributes(doc2, new FakeSpaceObject(2, "b"));

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

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(
				documentCollection,
				FakeMirroredDocumentConverter.create(),
				mirroredObject,
				SpaceObjectFilter.partitionFilter(mirroredObject, 1, 2));
		
		List<FakeSpaceObject> loadedSpaceObjects = documentLoader.loadAllObjects()
				  .stream()
				  .map(LoadedDocument::getDocument)
				  .collect(Collectors.toList());
		
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
