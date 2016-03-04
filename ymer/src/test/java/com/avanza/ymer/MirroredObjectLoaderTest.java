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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.MirroredObjectLoader.LoadedDocument;
import com.avanza.ymer.util.OptionalUtil;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.mongodb.BasicDBObject;

public class MirroredObjectLoaderTest {

	private MirrorContextProperties contextProperties = new MirrorContextProperties(1, 1);
	
	@Test
	public void loadsAllObjectsRoutedToCurrentPartition() {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();

		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 11);
		doc1.put("spaceRouting", 1);

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 22);
		doc2.put("spaceRouting", 2);

		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("spaceRouting", 2);
		doc3.put("patched", false);
		mirroredObject.setDocumentVersion(doc3, mirroredObject.getCurrentVersion());

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc1, doc2, doc3);

		SpaceObjectFilter.Impl<FakeSpaceObject> filterImpl = spaceObject -> {
			return spaceObject.getId() == doc2.getInt("_id") || spaceObject.getId() == doc3.getInt("_id");
		};
		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.create(filterImpl), contextProperties);

		List<FakeSpaceObject> loadedSpaceObjects = documentLoader.loadAllObjects().stream()
																 .map(LoadedDocument::getDocument)
																 .collect(Collectors.toList());
		assertEquals(2, loadedSpaceObjects.size());
		assertEquals(new FakeSpaceObject(22, 2, true), loadedSpaceObjects.get(0));
		assertEquals(new FakeSpaceObject(33, 2, false), loadedSpaceObjects.get(1));
	}

	@Test
	public void pendingPatchesDocumentsReturnsAllDocumentsThatWasPatched() {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();

		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 11);

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 22);

		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("patched", false);
		mirroredObject.setDocumentVersion(doc3, mirroredObject.getCurrentVersion());

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc1, doc2, doc3);

		SpaceObjectFilter.Impl<FakeSpaceObject> filterImpl = spaceObject -> {
			return spaceObject.getId() == doc2.getInt("_id") || spaceObject.getId() == doc3.getInt("_id");
		};
		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.create(filterImpl), contextProperties);
		List<PatchedDocument> patchedDocuments = documentLoader.loadAllObjects().stream()
																 .map(LoadedDocument::getPatchedDocument)
																 .flatMap(OptionalUtil::asStream)
																 .collect(Collectors.toList());
		
		assertEquals(1, patchedDocuments.size());
		assertEquals(doc2, patchedDocuments.get(0).getOldVersion());
		assertEquals(mirroredObject.patch(doc2), patchedDocuments.get(0).getNewVersion());
	}

	@Test
	public void loadsAndPatchesADocumentById() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();

		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("patched", false);

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc3);

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.acceptAll(), contextProperties);
		Optional<PatchedDocument> patchedDocument = documentLoader.loadById(doc3.get("_id"))
																  .flatMap(LoadedDocument::getPatchedDocument);
		
		assertTrue(patchedDocument.isPresent());
		assertEquals(doc3, patchedDocument.get().getOldVersion());
		assertEquals(mirroredObject.patch(doc3), patchedDocument.get().getNewVersion());
	}


	@Test(expected = IllegalArgumentException.class)
	public void loadByIdThrowsIllegalArgumentExceptionIfSpaceObjectNotAcceptedByFilter() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();

		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("patched", false);

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc3);

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.create(obj -> false), contextProperties);
		documentLoader.loadById(doc3.get("_id"));
	}

	@Test
	public void loadByIdReturnsEmptyOptionalIfNoDocumentFoundWithGivenId() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();
		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.acceptAll(), contextProperties);
		assertFalse(documentLoader.loadById("id_3").isPresent());
	}

	@Test
	public void reloadableSpaceObjectsAreMarkedAsRestored() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();

		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("patched", false);
		doc3.put("versionID", 1);

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc3);

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.acceptAll(), contextProperties);
		FakeSpaceObject spaceObject = documentLoader.loadById(doc3.get("_id"))
					  .map(LoadedDocument::getDocument)
					  .orElseThrow(() -> new AssertionError("Expected a reloaded document"));
					

		assertEquals(1, spaceObject.getVersionID());
		assertEquals(Integer.valueOf(1), spaceObject.getLatestRestoreVersion());
	}

	@Test(expected = RuntimeException.class)
	public void breaksIfConverterThrowsException() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();

		BasicDBObject doc1 = new BasicDBObject();
		doc1.put("_id", 11);
		doc1.put("spaceRouting", 1);

		final BasicDBObject doc2 = new BasicDBObject();
		doc2.put("_id", 22);
		doc2.put("spaceRouting", 2);

		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("spaceRouting", 2);
		doc3.put("patched", false);
		mirroredObject.setDocumentVersion(doc3, mirroredObject.getCurrentVersion());

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc1, doc2, doc3);

		SpaceObjectFilter.Impl<FakeSpaceObject> filterImpl = spaceObject -> {
			return spaceObject.getId() == doc2.getInt("_id") || spaceObject.getId() == doc3.getInt("_id");
		};
		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.createConverterWhichThrowsException(), mirroredObject, SpaceObjectFilter.create(filterImpl), contextProperties);
		documentLoader.loadAllObjects();
	}
	
	@Test(expected = RuntimeException.class)
	public void propagatesExceptionsThrownByMigrator() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() {
			@Override
			public void apply(BasicDBObject dbObject) {
				throw new IllegalArgumentException("My bigest failure");
			}
		} };
		MirroredObject<FakeSpaceObject> mirroredObject = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument();
		final BasicDBObject doc3 = new BasicDBObject();
		doc3.put("_id", 33);
		doc3.put("patched", false);

		FakeDocumentCollection fakeDocumentCollection = new FakeDocumentCollection();
		fakeDocumentCollection.insertAll(doc3);

		MirroredObjectLoader<FakeSpaceObject> documentLoader = new MirroredObjectLoader<>(fakeDocumentCollection, FakeMirroredDocumentConverter.create(), mirroredObject, SpaceObjectFilter.acceptAll(), contextProperties);
		documentLoader.loadById(doc3.get("_id"));
	}

	private static class FakeSpaceObject implements ReloadableSpaceObject {

		private int id;
		private boolean patched;
		// from ReloadableSpaceObject
		private int versionID;
		private Integer latestRestoreVersion;

		public FakeSpaceObject(int id, int spaceRouting, boolean patched) {
			this.id = id;
			this.patched = patched;
		}

		public FakeSpaceObject() {
		}

		public void setPatched(boolean patched) {
			this.patched = patched;
		}

		@SpaceId
		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
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
			return "FakeSpaceObject [id=" + id + ", patched=" + patched + "]";
		}

		@Override
		public int getVersionID() {
			return versionID;
		}

		@Override
		public void setVersionID(int versionID) {
			this.versionID = versionID;
		}

		@Override
		public Integer getLatestRestoreVersion() {
			return latestRestoreVersion;
		}

		@Override
		public void setLatestRestoreVersion(Integer latestRestoreVersion) {
			this.latestRestoreVersion = latestRestoreVersion;
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

	private static class FakeMirroredDocumentConverter implements DocumentConverter.Provider {

		@Override
		public <T> T convert(Class<T> toType, BasicDBObject document) {
			FakeSpaceObject spaceObject = new FakeSpaceObject();
			spaceObject.setPatched(document.getBoolean("patched"));
			spaceObject.setId(document.getInt("_id"));
			return toType.cast(spaceObject);
		}

		public static DocumentConverter createConverterWhichThrowsException() {
			return DocumentConverter.create(new FakeMirroredDocumentConverter() {
				@Override
				public <T> T convert(Class<T> toType, BasicDBObject document) {
					throw new RuntimeException("This converter always throws exception!");
				}
			});
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
