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
package se.avanzabank.mongodb.support.mirror;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Query;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperationType;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import se.avanzabank.mongodb.support.FakeDocumentWriteExceptionHandler;
import se.avanzabank.mongodb.support.mirror.VersionedMongoDbExternalDataSourceInitialLoadIntegrationTest.TestSpaceObjectV1Patch;


public class MirroredDocumentWriterTest {

	private MirroredDocumentWriter mirroredDocumentWriter;
	private DocumentConverter documentConverter;
	private DocumentDb documentDb;
	private MirroredDocument<TestSpaceObject> mirroredDocument;
	private MirroredDocument<TestSpaceOtherObject> anotherMirroredDocument;
	private MirroredDocument<TestReloadableSpaceObject> mirroredReloadableDocument;
	private SpaceMirrorContext mirror;
	private MirrorExceptionSpy mirrorExceptionSpy;
	private MirroredDocuments mirroredDocuments;

	@Before
	public void setup() {
		mirroredDocument = new MirroredDocument<>(TestSpaceObject.class, new TestSpaceObjectV1Patch());
		anotherMirroredDocument =
				MirroredDocument.createDocument(TestSpaceOtherObject.class, EnumSet.of(MirroredDocument.Flag.KeepPersistent));
		mirroredReloadableDocument = new MirroredDocument<>(TestReloadableSpaceObject.class);
		mirroredDocuments = new MirroredDocuments(mirroredDocument, mirroredReloadableDocument, anotherMirroredDocument);
		documentConverter = TestSpaceObjectFakeConverter.create();
		documentDb = FakeDocumentDb.create();
		mirrorExceptionSpy = new MirrorExceptionSpy();
		mirror = new SpaceMirrorContext(mirroredDocuments, documentConverter, documentDb, mirrorExceptionSpy);
		mirroredDocumentWriter = new MirroredDocumentWriter(mirror, new FakeDocumentWriteExceptionHandler());
	}

	@Test
	public void writesAreInsertedInDb() throws Exception {
		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.WRITE);

		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));


		BasicDBObject expected = documentConverter.convertToDBObject(item1);
		mirroredDocument.setDocumentVersion(expected, mirroredDocument.getCurrentVersion());

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(1, persisted.size());
		assertEquals(expected, persisted.get(0));
	}

	@Test
	public void writesAllObjectsInDb() throws Exception {
		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		TestSpaceObject item2 = new TestSpaceObject("2", "hello");
		TestSpaceObject item3 = new TestSpaceObject("3", "hello");
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(new FakeBulkItem(item1, BulkItem.WRITE),
																   new FakeBulkItem(item2, BulkItem.WRITE),
																   new FakeBulkItem(item3, BulkItem.WRITE)));

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(3, persisted.size());
	}

	@Test
	public void partialUpdatesAreUpdatedInDb() throws Exception {
		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		documentDb.getCollection(mirroredDocument.getCollectionName()).insert(documentConverter.convertToDBObject(item1));
		item1.setMessage("updated");
		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.PARTIAL_UPDATE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		BasicDBObject expected = documentConverter.convertToDBObject(item1);
		mirroredDocument.setDocumentVersion(expected, mirroredDocument.getCurrentVersion());

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(1, persisted.size());
		assertEquals(expected, persisted.get(0));
	}

	@Test
	public void udatesAreUpdatedInDb() throws Exception {
		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		documentDb.getCollection(mirroredDocument.getCollectionName()).insert(documentConverter.convertToDBObject(item1));
		item1.setMessage("updated");

		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.UPDATE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		BasicDBObject expected = documentConverter.convertToDBObject(item1);
		mirroredDocument.setDocumentVersion(expected, mirroredDocument.getCurrentVersion());

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(1, persisted.size());
		assertEquals(expected, persisted.get(0));
	}

	@Test
	public void removesDocumentFromDb() throws Exception {
		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		BasicDBObject dbObject = documentConverter.convertToDBObject(item1);
		documentDb.getCollection(mirroredDocument.getCollectionName()).insert(dbObject);

		TestSpaceObject bulkItemOBject = new TestSpaceObject("1", null);
		FakeBulkItem bulkItem = new FakeBulkItem(bulkItemOBject, BulkItem.REMOVE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(0, persisted.size());
	}

	@Test
	public void removesDocumentFromDb3() throws Exception {
		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		documentDb.getCollection(mirroredDocument.getCollectionName()).insert(documentConverter.convertToDBObject(item1));

		item1.setMessage("hello2");

		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.REMOVE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(0, persisted.size());
	}

	@Test
	public void documentsWithKeepPersistentFlagAreNotRemovedFromDb() throws Exception {
		TestSpaceOtherObject item1 = new TestSpaceOtherObject("1", "hello");
		documentDb.getCollection(mirroredDocument.getCollectionName()).insert(documentConverter.convertToDBObject(item1));

		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.REMOVE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));
		BasicDBObject expected = documentConverter.convertToDBObject(item1);

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(1, persisted.size());
		assertEquals(expected, persisted.get(0));
	}

	@Test
	public void ignoresNonMirroredTypes() throws Exception {
		class NonMirroredType {
		}
		FakeBulkItem bulkItem = new FakeBulkItem(new NonMirroredType(), BulkItem.WRITE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(0, persisted.size());
	}

	@Test
	public void notifiesMirrorExceptionListenerOnError() throws Exception {
		documentDb = throwsOnUpdateDocumentDb();
		mirrorExceptionSpy = new MirrorExceptionSpy();
		mirror = new SpaceMirrorContext(mirroredDocuments, documentConverter, documentDb, mirrorExceptionSpy);
		mirroredDocumentWriter = new MirroredDocumentWriter(mirror, new FakeDocumentWriteExceptionHandler());

		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.UPDATE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		assertNotNull(mirrorExceptionSpy.lastException);
		assertEquals(RuntimeException.class, mirrorExceptionSpy.lastException.getClass());
	}

	@Test(expected=DocumentWriteTransientException.class)
	public void exceptionFromExceptionHandlerIsPropagated() throws Exception {
		documentDb = throwsOnUpdateDocumentDb();
		mirrorExceptionSpy = new MirrorExceptionSpy();
		mirror = new SpaceMirrorContext(mirroredDocuments, documentConverter, documentDb, mirrorExceptionSpy);
		mirroredDocumentWriter = new MirroredDocumentWriter(mirror, new FakeDocumentWriteExceptionHandler(
				new DocumentWriteTransientException(new Exception())));

		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.UPDATE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));
	}

	@Test
	public void exceptionThrownDuringConvertionToMongoDbObjectAreNotPropagated() throws Exception {
		documentConverter = DocumentConverter.create(new DocumentConverter.Provider() {
			@Override
			public BasicDBObject convertToDBObject(Object type) {
				throw new RuntimeException("");
			}

			@Override
			public <T> T convert(Class<T> toType, BasicDBObject document) {
				throw new RuntimeException("");
			}

			@Override
			public Object convert(Object type) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Query toQuery(Object template) {
				throw new UnsupportedOperationException();
			}
		});
		mirrorExceptionSpy = new MirrorExceptionSpy();
		mirror = new SpaceMirrorContext(mirroredDocuments, documentConverter, documentDb, mirrorExceptionSpy);
		mirroredDocumentWriter = new MirroredDocumentWriter(mirror, new FakeDocumentWriteExceptionHandler());

		TestSpaceObject item1 = new TestSpaceObject("1", "hello");
		FakeBulkItem bulkItem = new FakeBulkItem(item1, BulkItem.UPDATE);
		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem));

		assertNotNull(mirrorExceptionSpy.lastException);
		assertEquals(RuntimeException.class, mirrorExceptionSpy.lastException.getClass());
	}

	@Test
	public void writesOfReloadedObjectsAreNotInsertedInDb() throws Exception {
		TestReloadableSpaceObject spaceObject = new TestReloadableSpaceObject(1, 1, false, 1, 1);
		TestReloadableSpaceObject spaceObject2 = new TestReloadableSpaceObject(2, 1, false, 2, 1);
		TestReloadableSpaceObject spaceObject3 = new TestReloadableSpaceObject(3, 1, false, 3, 2);
		FakeBulkItem bulkItem = new FakeBulkItem(spaceObject, BulkItem.WRITE);
		FakeBulkItem bulkItem2 = new FakeBulkItem(spaceObject2, BulkItem.WRITE);
		FakeBulkItem bulkItem3 = new FakeBulkItem(spaceObject3, BulkItem.WRITE);

		mirroredDocumentWriter.executeBulk(Arrays.<BulkItem>asList(bulkItem, bulkItem2, bulkItem3));


		BasicDBObject expected = documentConverter.convertToDBObject(spaceObject);
		mirroredReloadableDocument.setDocumentVersion(expected, mirroredReloadableDocument.getCurrentVersion());

		List<DBObject> persisted = Iterables.newArrayList(documentDb.getCollection(mirroredReloadableDocument.getCollectionName()).findAll(Optional.<SpaceObjectFilter<?>>empty()));
		assertEquals(2, persisted.size());
		assertEquals(2, persisted.get(0).get("_id"));
		assertEquals(3, persisted.get(1).get("_id"));
	}

	private DocumentDb throwsOnUpdateDocumentDb() {
		return DocumentDb.create(name -> new FakeDocumentCollection() {
			@Override
			public void update(BasicDBObject dbObject) {
				throw new RuntimeException();
			}
		});
	}

	static class MirrorExceptionSpy implements MirrorExceptionListener {

		private Exception lastException;
		private Object[] lastFailedObjects;
		private MirrorOperation lastFailedOperation;

		@Override
		public void onMirrorException(Exception e, MirrorOperation operation, Object[] failedObjects) {
			this.lastException = e;
			this.lastFailedObjects = failedObjects;
			this.lastFailedOperation = operation;
		}

	}

	public static class FakeBulkItem implements BulkItem {

		private final Object item;
		private final short operation;

		public FakeBulkItem(Object item, short operation) {
			this.item = item;
			this.operation = operation;
		}

		@Override
		public String getIdPropertyName() {
			return null;
		}

		@Override
		public Object getIdPropertyValue() {
			return null;
		}

		@Override
		public Object getItem() {
			return item;
		}

		@Override
		public Map<String, Object> getItemValues() {
			return null;
		}

		@Override
		public short getOperation() {
			return operation;
		}

		@Override
		public String getTypeName() {
			return null;
		}

		@Override
		public SpaceDocument getDataAsDocument() {
			return null;
		}

		@Override
		public Object getDataAsObject() {
			return null;
		}

		@Override
		public DataSyncOperationType getDataSyncOperationType() {
			return null;
		}

		@Override
		public Object getSpaceId() {
			return null;
		}

		@Override
		public SpaceTypeDescriptor getTypeDescriptor() {
			return null;
		}

		@Override
		public String getUid() {
			return null;
		}

		@Override
		public boolean supportsDataAsDocument() {
			return false;
		}

		@Override
		public boolean supportsDataAsObject() {
			return false;
		}

		@Override
		public boolean supportsGetSpaceId() {
			return false;
		}

		@Override
		public boolean supportsGetTypeDescriptor() {
			return false;
		}
	}

}
