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
package com.avanza.gs.mongo.mirror;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.gs.mongo.DocumentWriteExceptionHandler;
import com.avanza.gs.mongo.util.Require;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;
import com.gigaspaces.sync.OperationsBatchData;
import com.mongodb.BasicDBObject;

/**
 * 
 * @author Elias Lindholm (elilin)
 * 
 */
final class MirroredDocumentWriter {

	private static final Logger logger = LoggerFactory.getLogger(MirroredDocumentWriter.class);

	private final SpaceMirrorContext mirror;
	private final DocumentWriteExceptionHandler exceptionHandler;

	MirroredDocumentWriter(SpaceMirrorContext mirror, DocumentWriteExceptionHandler exceptionHandler) {
		this.mirror = Objects.requireNonNull(mirror);
		this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
	}

	public void executeBulk(OperationsBatchData batch) {
		List<Object> pendingWrites = new ArrayList<Object>();
		for (DataSyncOperation bulkItem : filterSpaceObjects(batch.getBatchDataItems())) {
			if (!mirror.isMirroredType(bulkItem.getDataAsObject().getClass())) {
				logger.debug("Ignored {}, not a mirrored class", bulkItem.getDataAsObject().getClass().getName());
				continue;
			}
			switch (bulkItem.getDataSyncOperationType()) {
			case WRITE:
				pendingWrites.add(bulkItem.getDataAsObject());
				break;
			case UPDATE:
			case PARTIAL_UPDATE:
				insertAll(pendingWrites);
				pendingWrites = new ArrayList<>();
				update(bulkItem.getDataAsObject());
				break;
			case REMOVE:
				insertAll(pendingWrites);
				pendingWrites = new ArrayList<>();
				remove(bulkItem.getDataAsObject());
				break;
			default:
				throw new UnsupportedOperationException("Bulkoperation " + bulkItem.getDataSyncOperationType()
						+ " is not supported");
			}
		}
		insertAll(pendingWrites);
	}

	private Collection<DataSyncOperation> filterSpaceObjects(DataSyncOperation[] batchDataItems) {
		ArrayList<DataSyncOperation> result = new ArrayList<>(batchDataItems.length);
		for (DataSyncOperation bulkItem : batchDataItems) {
			if (isReloaded(bulkItem)) {
				continue;
			}
			if (bulkItem.getDataSyncOperationType() == DataSyncOperationType.REMOVE
					&& mirror.keepPersistent(bulkItem.getDataAsObject().getClass())) {
				continue;
			}
			result.add(bulkItem);
		}
		return result;
	}

	private boolean isReloaded(DataSyncOperation bulkItem) {
		Object item = bulkItem.getDataAsObject();
		return (bulkItem.getDataSyncOperationType() == DataSyncOperationType.WRITE || bulkItem.getDataSyncOperationType() == DataSyncOperationType.UPDATE)
				&& item instanceof ReloadableSpaceObject
				&& ReloadableSpaceObjectUtil.isReloaded((ReloadableSpaceObject) item);
	}

	public void executeBulk(List<BulkItem> bulkItems) throws DataSourceException {
		List<Object> pendingWrites = new ArrayList<Object>();
		for (BulkItem bulkItem : filterSpaceObjects(bulkItems)) {
			if (!mirror.isMirroredType(bulkItem.getItem().getClass())) {
				logger.debug("Ignored {}, not a mirrored class", bulkItem.getItem().getClass().getName());
				continue;
			}
			switch (bulkItem.getOperation()) {
			case BulkItem.WRITE:
				pendingWrites.add(bulkItem.getItem());
				break;
			case BulkItem.UPDATE:
			case BulkItem.PARTIAL_UPDATE:
				insertAll(pendingWrites);
				pendingWrites = new ArrayList<>();
				update(bulkItem.getItem());
				break;
			case BulkItem.REMOVE:
				insertAll(pendingWrites);
				pendingWrites = new ArrayList<>();
				remove(bulkItem.getItem());
				break;
			default:
				throw new UnsupportedOperationException("Bulkoperation " + bulkItem.getOperation()
						+ " is not supported");
			}
		}
		insertAll(pendingWrites);
	}

	private List<BulkItem> filterSpaceObjects(List<BulkItem> bulkItems) {
		ArrayList<BulkItem> result = new ArrayList<>(bulkItems.size());
		for (BulkItem bulkItem : bulkItems) {
			if (isReloaded(bulkItem)) {
				continue;
			}
			if (bulkItem.getOperation() == BulkItem.REMOVE
				&& mirror.keepPersistent(bulkItem.getItem().getClass())) {
				continue;
			}
			result.add(bulkItem);
		}
		return result;
	}

	private boolean isReloaded(BulkItem bulkItem) {
		Object item = bulkItem.getItem();
		return (bulkItem.getOperation() == BulkItem.WRITE || bulkItem.getOperation() == BulkItem.UPDATE)
				&& item instanceof ReloadableSpaceObject
				&& ReloadableSpaceObjectUtil.isReloaded((ReloadableSpaceObject) item);
	}

	private void remove(final Object item) {
		new MongoCommand(MirrorOperation.REMOVE, item) {
			@Override
			protected void execute(BasicDBObject... dbOBject) {
				BasicDBObject id = new BasicDBObject();
				id.put("_id", dbOBject[0].get("_id"));
				getDocumentCollection(item).delete(id);
			}

		}.execute(item);
	}

	private void update(final Object item) {
		new MongoCommand(MirrorOperation.UPDATE, item) {
			@Override
			protected void execute(BasicDBObject... dbOBject) {
				getDocumentCollection(item).update(dbOBject[0]);
			}

		}.execute(item);
	}

	private void insertAll(List<Object> items) {
		Map<String, List<Object>> pendingItemsByCollection = new HashMap<>();
		for (Object item : items) {
			String collectionName = this.mirror.getCollectionName(item.getClass());
			List<Object> documentToBeWrittenToCollection = pendingItemsByCollection.get(collectionName);
			if (null == documentToBeWrittenToCollection) {
				documentToBeWrittenToCollection = new ArrayList<Object>();
				pendingItemsByCollection.put(collectionName, documentToBeWrittenToCollection);
			}
			documentToBeWrittenToCollection.add(item);
		}
		for (final List<Object> pendingObjects : pendingItemsByCollection.values()) {
			new MongoCommand(MirrorOperation.INSERT, pendingObjects) {
				@Override
				protected void execute(BasicDBObject... dbObjects) {
					DocumentCollection documentCollection = getDocumentCollection(pendingObjects.get(0));
					documentCollection.insertAll(dbObjects);
				}
			}.execute(pendingObjects.toArray());
		}
	}

	private BasicDBObject toDbObject(Object item) {
		return this.mirror.toVersionedDbObject(item);
	}

	private DocumentCollection getDocumentCollection(Object item) {
		return this.mirror.getDocumentCollection(item.getClass());
	}

	abstract class MongoCommand {

		private final Object[] objects;
		private final MirrorOperation operation;

		public MongoCommand(MirrorOperation operation, Object... objects) {
			this.objects = objects;
			this.operation = operation;
		}

		final void execute(Object... items) {
			try {
				BasicDBObject[] documents = new BasicDBObject[items.length];
				for (int i = 0; i < documents.length; i++) {
					documents[i] = toDbObject(items[i]);
				}
				execute(documents);
			} catch (Exception e) {
				onException(e);
			}
		}

		private void onException(final Exception exception) {
			mirror.onMirrorException(exception, operation, objects);
			exceptionHandler.handleException(exception,
					"Operation: " + operation + ", objects: " + Arrays.toString(objects));
		}

		protected abstract void execute(BasicDBObject... dbOBject);

	}

}
