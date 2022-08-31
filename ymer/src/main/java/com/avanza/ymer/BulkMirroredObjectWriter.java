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
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;

final class BulkMirroredObjectWriter {

	private static final Logger logger = LoggerFactory.getLogger(BulkMirroredObjectWriter.class);

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

		changesByCollection.forEach((collectionName, bulkChanges) -> {
			List<MongoBulkChange> remainingChanges = bulkChanges;
			while (!remainingChanges.isEmpty()) {
				remainingChanges = executeMongoDbBulk(collectionName, metadata, remainingChanges);
			}
		});
	}

	/**
	 * Executes a bulkWrite against mongoDB, with possibility to retry if an operation fails.
	 *
	 * @return list of changes that weren't written and needs to be retried.
	 *         This happens if a row in a bulkWrite failed and needs to be skipped.
	 */
	private List<MongoBulkChange> executeMongoDbBulk(String collectionName, InstanceMetadata metadata, List<MongoBulkChange> changes) {
		Map<Integer, Integer> bulkChangeIdToChangeMap = new HashMap<>();
		try {
			DocumentCollection collection = mirror.getDocumentCollection(collectionName);

			AtomicInteger bulkChangeId = new AtomicInteger(0);
			LongAdder updates = new LongAdder();
			LongAdder removals = new LongAdder();

			BulkWriteResult result = collection.orderedBulkWrite(bulkWriter -> {
				for (int i = 0; i < changes.size(); i++) {
					MongoBulkChange change = changes.get(i);

					Document versionedDocument;
					try {
						versionedDocument = mirror.toVersionedDocument(change.object, metadata);
						mirror.getPreWriteProcessing(change.object.getClass()).preWrite(versionedDocument);
					} catch (Exception e) {
						mirror.onMirrorException(e, change.operation, change.object);
						exceptionHandler.handleException(e, "Conversion failed, operation: " + change.operation + ", change: " + change.object);
						continue;
					}

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

					// keep track of which id in the MongoDB bulk maps to which index in this list as some items might be skipped
					bulkChangeIdToChangeMap.put(bulkChangeId.getAndIncrement(), i);
				}
			});

			addResultToStatistics(result);
			checkBulkResultForWarnings(updates.intValue(), removals.intValue(), result);

			return emptyList();
		} catch (MongoBulkWriteException e) {
			addResultToStatistics(e.getWriteResult());

			BulkWriteError writeError = e.getWriteErrors().get(0); // always a single write error as we use an ordered operation
			MongoBulkChange failedChange = changes.get(writeError.getIndex());
			mirror.onMirrorException(e, failedChange.operation, failedChange.object);

			int failedChangeIndex = bulkChangeIdToChangeMap.get(writeError.getIndex());
			List<MongoBulkChange> remainingChanges = changes.subList(failedChangeIndex + 1, changes.size());

			if (!remainingChanges.isEmpty()) {
				logger.error("Bulk write failed on a {} operation in collection {}: \"{}\". Will continue writing remaining {} changes",
						failedChange.operation, collectionName, writeError.getMessage(), remainingChanges.size());
			} else {
				logger.error("Bulk write failed on a {} operation in collection {}: \"{}\". This was the last entry in the batch",
						failedChange.operation, collectionName, writeError.getMessage(), e);
			}

			return remainingChanges;
		} catch (Exception e) {
			exceptionHandler.handleException(e, "Operation: Bulk write, changes: " + changes);
			return emptyList();
		}
	}

	private void addResultToStatistics(BulkWriteResult result) {
		operationsListener.increment(PerformedOperationsListener.OperationType.INSERT, result.getInsertedCount());
		operationsListener.increment(PerformedOperationsListener.OperationType.UPDATE, result.getMatchedCount());
		operationsListener.increment(PerformedOperationsListener.OperationType.DELETE, result.getDeletedCount());
	}

	private void checkBulkResultForWarnings(int expectedUpdates, int expectedRemovals, BulkWriteResult result) {
		if (expectedUpdates != result.getMatchedCount()) {
			StringBuilder warningMessage = new StringBuilder();
			warningMessage.append("Tried to update ").append(expectedUpdates).append(" documents in current bulk write, but ");
			if (result.getMatchedCount() > 0) {
				warningMessage.append("only ").append(result.getMatchedCount()).append(" ");
			} else {
				warningMessage.append("none ");
			}
			warningMessage.append("were matched by query. MongoDB and space seems to be out of sync! ");
			if (!result.getUpserts().isEmpty()) {
				warningMessage.append("The following ids were inserted into MongoDB as a result of update operations: [")
						.append(result.getUpserts().stream()
								.map(bson -> bson.getId().asString().getValue())
								.collect(joining(", ")))
						.append("].");
			} else {
				warningMessage.append("No rows were inserted into MongoDB as a result of this query.");
			}

			logger.warn(warningMessage.toString());
		}
		if (expectedRemovals != result.getDeletedCount()) {
			logger.warn("Tried to delete {} documents in current bulk write, but {} were deleted by query. "
					+ "MongoDB and space seems to be out of sync!", expectedRemovals,
					result.getDeletedCount() > 0 ? "only " + result.getDeletedCount() :  "none");
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
