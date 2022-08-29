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

import static com.avanza.ymer.PerformedOperationsListener.OperationType.READ_BATCH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.ymer.PerformedOperationsListener.OperationType;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;

final class BulkMirroredObjectWriter {

	private static final Logger logger = LoggerFactory.getLogger(BulkMirroredObjectWriter.class);

	private static final int MAX_BULK_RETRIES = 100;

	private final SpaceMirrorContext mirror;
	private final DocumentWriteExceptionHandler exceptionHandler;
	private final MirroredObjectFilterer objectFilterer;
	private final PerformedOperationsListener operationsListener;

	BulkMirroredObjectWriter(SpaceMirrorContext mirror,
			DocumentWriteExceptionHandler exceptionHandler,
			MirroredObjectFilterer objectFilterer,
			PerformedOperationsListener operationsListener
	) {
		this.mirror = requireNonNull(mirror);
		this.exceptionHandler = requireNonNull(exceptionHandler);
		this.objectFilterer = requireNonNull(objectFilterer);
		this.operationsListener = requireNonNull(operationsListener);
	}

	public void executeBulk(InstanceMetadata metadata, OperationsBatchData batch) {
		operationsListener.increment(READ_BATCH, batch.getBatchDataItems().length);
		Map<String, List<MongoBulkChange>> changesByCollection = new HashMap<>();

		for (DataSyncOperation bulkItem : objectFilterer.filterSpaceObjects(batch.getBatchDataItems())) {
			String collectionName = mirror.getCollectionName(bulkItem.getDataAsObject().getClass());
			List<MongoBulkChange> bulkChanges = changesByCollection.computeIfAbsent(collectionName, x -> new ArrayList<>());

			switch (bulkItem.getDataSyncOperationType()) {
				case WRITE:
					bulkChanges.add(new MongoBulkChange(MirrorOperation.INSERT, bulkItem.getDataAsObject()));
					break;
				case UPDATE:
				case PARTIAL_UPDATE:
					bulkChanges.add(new MongoBulkChange(MirrorOperation.UPDATE, bulkItem.getDataAsObject()));
					break;
				case REMOVE:
					bulkChanges.add(new MongoBulkChange(MirrorOperation.REMOVE, bulkItem.getDataAsObject()));
					break;
				default:
					throw new UnsupportedOperationException("Bulkoperation " + bulkItem.getDataSyncOperationType() + " is not supported");
			}
		}

		changesByCollection.forEach((collectionName, bulkChanges) -> executeBulk(collectionName, metadata, bulkChanges, 0));
	}

	private void executeBulk(String collectionName, InstanceMetadata metadata, List<MongoBulkChange> changes, int currentRetry) {
		try {
			DocumentCollection collection = mirror.getDocumentCollection(collectionName);

			LongAdder updates = new LongAdder();
			LongAdder removals = new LongAdder();

			BulkWriteResult result = collection.orderedBulkWrite(bulkWriter -> {
				changes.forEach(change -> {
					Document versionedDocument = mirror.toVersionedDocument(change.object, metadata);
					mirror.getPreWriteProcessing(change.object.getClass()).preWrite(versionedDocument);

					switch (change.operation) {
						case INSERT:
							bulkWriter.insert(versionedDocument);
							break;
						case UPDATE:
							bulkWriter.replace(versionedDocument);
							updates.increment();
							break;
						case REMOVE:
							bulkWriter.delete(versionedDocument);
							removals.increment();
							break;
					}
				});
			});

			addResultToStatistics(result);
			checkResultForWarnings(updates.intValue(), removals.intValue(), result);

		} catch (MongoBulkWriteException e) {
			addResultToStatistics(e.getWriteResult());

			BulkWriteError writeError = e.getWriteErrors().get(0); // always a single write error as we use an ordered operation
			MongoBulkChange failedChange = changes.get(writeError.getIndex());
			mirror.onMirrorException(e, failedChange.operation, failedChange.object);

			List<MongoBulkChange> remainingChanges = changes.subList(writeError.getIndex() + 1, changes.size());

			if (!remainingChanges.isEmpty()) {
				int nextRetry = currentRetry + 1;
				if (nextRetry > MAX_BULK_RETRIES) {
					exceptionHandler.handleException(e, "Bulk write failed on a " + failedChange.operation + ". This bulk has failed " + currentRetry + " retries, "
							+ "remaining changes that will NOT be attempted: " + remainingChanges);
				} else {
					logger.error("Bulk write failed on a {} operation in collection {}: \"{}\". Will continue writing remaining {} changes (retry #{})",
							failedChange.operation, collectionName, writeError.getMessage(), remainingChanges.size(), nextRetry);
					// Continue execution, skipping the failed operation
					executeBulk(collectionName, metadata, remainingChanges, nextRetry);
				}
			} else {
				logger.error("Bulk write failed on a {} operation in collection {}: \"{}\". This was the last entry in the batch",
						failedChange.operation, collectionName, writeError.getMessage(), e);
			}
		} catch (Exception e) {
			exceptionHandler.handleException(e, "Operation: Bulk write, changes: " + changes);
		}
	}

	private void addResultToStatistics(BulkWriteResult result) {
		operationsListener.increment(OperationType.INSERT, result.getInsertedCount());
		operationsListener.increment(OperationType.UPDATE, result.getMatchedCount());
		operationsListener.increment(OperationType.DELETE, result.getDeletedCount());
	}

	private void checkResultForWarnings(int expectedUpdates, int expectedRemovals, BulkWriteResult result) {
		if (expectedUpdates != result.getMatchedCount()) {
			logger.warn("Tried to update {} documents in current bulk write, but only {} were matched by query. "
							+ "MongoDB and space seems to be out of sync! "
							+ "The following ids were inserted into MongoDB as a result of update operations: [{}].",
					expectedUpdates, result.getMatchedCount(),
					result.getUpserts().stream()
							.map(bson -> bson.getId().asString().getValue())
							.collect(joining(", ")));
		}
		if (expectedRemovals != result.getDeletedCount()) {
			logger.warn("Tried to delete {} documents in current bulk write, but only {} were deleted by query. "
					+ "MongoDB and space seems to be out of sync!", expectedRemovals, result.getDeletedCount());
		}
	}

	private static class MongoBulkChange {
		private final MirrorOperation operation;
		private final Object object;

		MongoBulkChange(MirrorOperation operation, Object object) {
			this.operation = operation;
			this.object = object;
		}

		@Override
		public String toString() {
			return operation + ": " + object;
		}
	}
}
