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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;
import com.gigaspaces.sync.OperationsBatchData;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Elias Lindholm (elilin)
 */
final class MirroredObjectWriter {

	private static final Logger logger = LoggerFactory.getLogger(MirroredObjectWriter.class);

	private final SpaceMirrorContext mirror;
	private final DocumentWriteExceptionHandler exceptionHandler;
	private final Thread statusThread;

	private AtomicLong numInserts = new AtomicLong();
	private AtomicLong numUpdates = new AtomicLong();
	private AtomicLong numDeletes = new AtomicLong();

	MirroredObjectWriter(SpaceMirrorContext mirror, DocumentWriteExceptionHandler exceptionHandler) {
		this.mirror = Objects.requireNonNull(mirror);
		this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
		this.statusThread = new Thread(this::runStatusThread, "mirrored-object-writer-status");
		this.statusThread.setDaemon(true);
		this.statusThread.start();
	}

	private void runStatusThread() {
		long lastNumInserts = 0;
		long lastNumUpdates = 0;
		long lastNumDeletes = 0;
		long lastLogTime = System.currentTimeMillis();
		while (true) {
			try {
				Thread.sleep(10_000);
				if (lastNumInserts != numInserts.get() ||
						lastNumUpdates != numUpdates.get() ||
						lastNumDeletes != numDeletes.get()) {
					long inserts = numInserts.get();
					long updates = numUpdates.get();
					long deletes = numDeletes.get();
					long logTime = System.currentTimeMillis();
					long elapsedTime = (logTime - lastLogTime) / 1000;
					logger.info("STATUS: inserts={}, insertDelta={}, insertRate={}/s, updates={}, updateDelta={}, updateRate={}/s, deletes={}, deleteDelta={}, deleteRate={}/s",
							inserts, inserts - lastNumInserts, (inserts - lastNumInserts) / elapsedTime,
							updates, updates - lastNumUpdates, (updates - lastNumUpdates) / elapsedTime,
							deletes, deletes - lastNumDeletes, (deletes - lastNumDeletes) / elapsedTime);
					lastNumInserts = inserts;
					lastNumUpdates = updates;
					lastNumDeletes = deletes;
					lastLogTime = logTime;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (Exception e) {
				logger.warn("Exception occurred in status thread", e);
			}
		}
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

	private void remove(final Object item) {
		new MongoCommand(MirrorOperation.REMOVE, item) {
			@Override
			protected void execute(DBObject... dbOBject) {
				BasicDBObject id = new BasicDBObject();
				id.put("_id", dbOBject[0].get("_id"));
				getDocumentCollection(item).delete(id);
			}

		}.execute(item);
		numDeletes.incrementAndGet();
	}

	private void update(final Object item) {
		new MongoCommand(MirrorOperation.UPDATE, item) {
			@Override
			protected void execute(DBObject... dbOBject) {
				getDocumentCollection(item).update(dbOBject[0]);
			}

		}.execute(item);
		numUpdates.incrementAndGet();
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
				protected void execute(DBObject... dbObjects) {
					DocumentCollection documentCollection = getDocumentCollection(pendingObjects.get(0));
					documentCollection.insertAll(dbObjects);
				}
			}.execute(pendingObjects.toArray());
			numInserts.addAndGet(pendingObjects.size());
		}
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
				DBObject[] documents = new DBObject[items.length];
				for (int i = 0; i < documents.length; i++) {
					BasicDBObject versionedDbObject = MirroredObjectWriter.this.mirror.toVersionedDbObject(items[i]);
					MirroredObjectWriter.this.mirror.getPreWriteProcessing(items[i].getClass()).preWrite(versionedDbObject);
					documents[i] = versionedDbObject;
				}
				execute(documents);
			} catch (Exception e) {
				onException(e);
			}
		}

		private void onException(final Exception exception) {
			mirror.onMirrorException(exception, operation, objects);
			Map<String, List<Object>> objectsPerType = Stream.of(this.objects)
					.collect(Collectors.groupingBy(o -> o.getClass().getSimpleName()));
			exceptionHandler.handleException(exception,
					"Operation: " + operation + ", objects: " + objectsPerType);
		}

		protected abstract void execute(DBObject... dbOBject);

	}

}
