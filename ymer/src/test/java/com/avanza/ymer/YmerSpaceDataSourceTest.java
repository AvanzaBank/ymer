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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.stream.Stream;

import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.DocumentCollection;
import com.avanza.ymer.DocumentConverter;
import com.avanza.ymer.DocumentDb;
import com.avanza.ymer.DocumentPatch;
import com.avanza.ymer.MirroredObject;
import com.avanza.ymer.MirroredObjects;
import com.avanza.ymer.YmerSpaceDataSource;
import com.avanza.ymer.SpaceMirrorContext;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class YmerSpaceDataSourceTest {

	private final Integer partitionId = 1;
	private final int numberOfInstances = 2;

	@Test
	public void documentsMustNotBeWrittenToDbBeforeAllElementsAreLoaded() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> patchedMirroredDocument = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();
		DocumentDb fakeDb = FakeDocumentDb.create();
		SpaceMirrorContext spaceMirror = new SpaceMirrorContext(new MirroredObjects(patchedMirroredDocument), FakeDocumentConverter.create(), fakeDb);
		YmerSpaceDataSource ymerSpaceDataSource = new YmerSpaceDataSource(spaceMirror);
		ymerSpaceDataSource.setClusterInfo(new ClusterInfo("", partitionId, null, numberOfInstances, 0));

		DocumentCollection documentCollection = fakeDb.getCollection(patchedMirroredDocument.getCollectionName());
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 1);
		doc1.put("spaceRouting", 1);

		BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 2);
		doc2.put("spaceRouting", 2);

		BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 3);
		doc3.put("spaceRouting", 3);

		documentCollection.insert(doc1);
		documentCollection.insert(doc2);
		documentCollection.insert(doc3);

		Stream<FakeSpaceObject> loadInitialLoadData = ymerSpaceDataSource.load(patchedMirroredDocument);
		assertEquals(1, Iterables.sizeOf(loadInitialLoadData));
	}

	@Test
	public void loadsAndPatchesASingleDocumentById() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<TestReloadableSpaceObject> mirroredObject = MirroredObjectDefinition.create(TestReloadableSpaceObject.class).documentPatches(patches).buildMirroredDocument();
		DocumentDb documentDb = FakeDocumentDb.create();
		SpaceMirrorContext spaceMirror = new SpaceMirrorContext(
				new MirroredObjects(mirroredObject),
				TestSpaceObjectFakeConverter.create(),
				documentDb);
		YmerSpaceDataSource externalDataSourceForPartition1 = new YmerSpaceDataSource(spaceMirror);
		externalDataSourceForPartition1.setClusterInfo(new ClusterInfo("", partitionId, null, numberOfInstances, 0));

		DocumentCollection documentCollection = documentDb.getCollection(mirroredObject.getCollectionName());
		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 1);
		doc1.put("spaceRouting", 1);
		doc1.put("versionID", 1);

		BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 2);
		doc2.put("spaceRouting", 2);
		doc2.put("versionID", 1);

		BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 3);
		doc3.put("spaceRouting", 3);
		doc3.put("versionID", 1);

		documentCollection.insert(doc1);
		documentCollection.insert(doc2);
		documentCollection.insert(doc3);
		assertNotNull(externalDataSourceForPartition1.reloadObject(TestReloadableSpaceObject.class, 2));

		DBObject dbObject = documentDb.getCollection(mirroredObject.getCollectionName()).findById(2);
		assertFalse(mirroredObject.requiresPatching(new BasicDBObject(dbObject.toMap())));
	}


	private static class FakeSpaceObject {

		private int id;
		private int spaceRouting;

		public FakeSpaceObject() {
		}

		public void setId(int id) {
			this.id = id;
		}

		@SpaceRouting
		public int getSpaceRouting() {
			return spaceRouting;
		}

		public void setSpaceRouting(int routingKey) {
			this.spaceRouting = routingKey;
		}

		@Override
		public int hashCode() {
			return toString().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return toString().equals(obj.toString());
		}

		@Override
		public String toString() {
			return "FakeSpaceObject [id=" + id + ", spaceRouting=" + spaceRouting + "]";
		}

	}

	private static class FakeSpaceObjectV1Patch implements DocumentPatch {

		@Override
		public void apply(BasicDBObject dbObject) {
			dbObject.put("patched", true);
		}

		@Override
		public int patchedVersion() {
			return 1;
		}

	}

	private static class FakeDocumentConverter implements DocumentConverter.Provider {

		@SuppressWarnings("unchecked")
		@Override
		public <T> T convert(Class<T> toType, BasicDBObject document) {
			FakeSpaceObject spaceObject = new FakeSpaceObject();
			spaceObject.setSpaceRouting(document.getInt("spaceRouting"));
			spaceObject.setId(document.getInt("_id"));
			return (T) spaceObject;
		}

		@Override
		public BasicDBObject convertToDBObject(Object type) {
			throw new UnsupportedOperationException();
		}

		public static DocumentConverter create() {
			return DocumentConverter.create(new FakeDocumentConverter());
		}

		@Override
		public Object convert(Object type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Query toQuery(Object template) {
			throw new UnsupportedOperationException();
		}
	}

}
