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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.MirroredObjectLoader.LoadedDocument;
import com.avanza.ymer.plugin.PostReadProcessor;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MongoDocumentCollectionTest extends DocumentCollectionContract {

	private static final String DBNAME = "testdb";
	private static final String COLLECTION_NAME = "testcollection";

	private MongoDatabase database;
	private MongoCollection<Document> collection;
	private MongoServer mongoServer;
	private MongoClient mongoClient;

	public MongoDocumentCollectionTest() {
		mongoServer = new MongoServer(new MemoryBackend());
		InetSocketAddress serverAddress = mongoServer.bind();
		mongoClient = new MongoClient(new ServerAddress(serverAddress));
	}

	/* (non-Javadoc)
	 * @see com.avanza.ymer.DocumentCollectionContract#createEmptyCollection()
	 */
	@Override
	protected DocumentCollection createEmptyCollection() {
		database = mongoClient.getDatabase(DBNAME);
		database.drop();
		database = mongoClient.getDatabase(DBNAME);

		collection = database.getCollection(COLLECTION_NAME);

		return new MongoDocumentCollection(collection);
	}

	@Test
	public void canLoadDocumentsRouted() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).loadDocumentsRouted(true).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		// Objects WITH routed field
		Document doc1 = new Document();
		doc1.put("_id", 1);
		doc1.put("value", "a");
		mirroredObject.setDocumentAttributes(doc1, new FakeSpaceObject(1, "a"), 2);

		final Document doc2 = new Document();
		doc2.put("_id", 2);
		doc2.put("value", "b");
		mirroredObject.setDocumentAttributes(doc2, new FakeSpaceObject(2, "b"), 2);

		// Objects WITHOUT routed field
		final Document doc3 = new Document();
		doc3.put("_id", 3);
		doc3.put("value", "c");

		final Document doc4 = new Document();
		doc4.put("_id", 4);
		doc4.put("value", "d");

		MongoDocumentCollectionTest testCollection = new MongoDocumentCollectionTest();
		DocumentCollection documentCollection = testCollection.createEmptyCollection();
		documentCollection.insertAll(doc1, doc2, doc3, doc4);

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(
				documentCollection,
				FakeMirroredDocumentConverter.create(),
				mirroredObject,
				SpaceObjectFilter.partitionFilter(mirroredObject, 2, 2),
				new MirrorContextProperties(2, 2),
				noOpPostReadProcessor());

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
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class)
																			     .loadDocumentsRouted(true)
																				 .writeBackPatchedDocuments(false)
																				 .documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		// Objects WITH routed field
		Document doc1 = new Document();
		doc1.put("_id", 1);
		doc1.put("value", "a");
		mirroredObject.setDocumentAttributes(doc1, new FakeSpaceObject(1, "a"), 2);

		final Document doc2 = new Document();
		doc2.put("_id", 2);
		doc2.put("value", "b");
		mirroredObject.setDocumentAttributes(doc2, new FakeSpaceObject(2, "b"), 2);

		// Objects WITHOUT routed field
		final Document doc3 = new Document();
		doc3.put("_id", 3);
		doc3.put("value", "c");

		final Document doc4 = new Document();
		doc4.put("_id", 4);
		doc4.put("value", "d");

		MongoDocumentCollectionTest testCollection = new MongoDocumentCollectionTest();
		DocumentCollection documentCollection = testCollection.createEmptyCollection();
		documentCollection.insertAll(doc1, doc2, doc3, doc4);

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(
				documentCollection,
				FakeMirroredDocumentConverter.create(),
				mirroredObject,
				SpaceObjectFilter.partitionFilter(mirroredObject, 1, 2),
				new MirrorContextProperties(2, 1),
				noOpPostReadProcessor());

		List<FakeSpaceObject> loadedSpaceObjects = documentLoader.loadAllObjects()
				  .stream()
				  .map(LoadedDocument::getDocument)
				  .collect(Collectors.toList());

		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(2, "b")));
		assertTrue(loadedSpaceObjects.toString(), loadedSpaceObjects.contains(new FakeSpaceObject(4, "d")));
		assertEquals(2, loadedSpaceObjects.size());
	}

	static class FakeSpaceObject {
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
		public <T> T convert(Class<T> toType, Document document) {
			Integer id = Optional.ofNullable(document.getInteger("_id"))
								 .orElseThrow(NullPointerException::new);
			FakeSpaceObject spaceObject = new FakeSpaceObject(id, document.getString("value"));
			return toType.cast(spaceObject);
		}

		@Override
		public Document convertToBsonDocument(Object type) {
			return null;
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

	private PostReadProcessor noOpPostReadProcessor() {
		return (postRead) -> postRead;
	}

}
