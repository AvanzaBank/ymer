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

import static com.avanza.ymer.util.GigaSpacesPartitionIdUtil.extractPartitionIdFromSpaceName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;
import com.gigaspaces.sync.OperationsBatchData;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MirroredObjectWriter {

	private static final Logger logger = LoggerFactory.getLogger(MirroredObjectWriter.class);

	private final SpaceMirrorContext mirror;
	private final DocumentWriteExceptionHandler exceptionHandler;

	MirroredObjectWriter(SpaceMirrorContext mirror, DocumentWriteExceptionHandler exceptionHandler) {
		this.mirror = Objects.requireNonNull(mirror);
		this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
	}

	public void executeBulk(OperationsBatchData batch) {
		Integer partitionId = extractPartitionIdFromSpaceName(batch.getSourceDetails().getName()).orElse(null);
		List<Object> pendingWrites = new ArrayList<>();
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
					insertAll(partitionId, pendingWrites);
					pendingWrites = new ArrayList<>();
					update(partitionId, bulkItem.getDataAsObject());
					break;
				case REMOVE:
					insertAll(partitionId, pendingWrites);
					pendingWrites = new ArrayList<>();
					remove(partitionId, bulkItem.getDataAsObject());
					break;
				default:
					throw new UnsupportedOperationException("Bulkoperation " + bulkItem.getDataSyncOperationType() + " is not supported");
			}
		}
		insertAll(partitionId, pendingWrites);
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

	private void remove(@Nullable Integer partitionId, final Object item) {
		MongoCommand mongoCommand = new MongoCommand(MirrorOperation.REMOVE, partitionId, item) {
			@Override
			protected void execute(Document... documents) {
				Document id = new Document();
				id.put("_id", documents[0].get("_id"));
				getDocumentCollection(item).delete(id);
			}

		};
		mongoCommand.execute(item);
	}

	private void update(@Nullable Integer partitionId, final Object item) {
		new MongoCommand(MirrorOperation.UPDATE, partitionId, item) {
			@Override
			protected void execute(Document... documents) {
				getDocumentCollection(item).update(documents[0]);
			}
		}.execute(item);
	}

	private void insertAll(@Nullable Integer partitionId, List<Object> items) {
		Map<String, List<Object>> pendingItemsByCollection = new HashMap<>();
		for (Object item : items) {
			String collectionName = this.mirror.getCollectionName(item.getClass());
			List<Object> documentToBeWrittenToCollection =
					pendingItemsByCollection.computeIfAbsent(collectionName, k -> new ArrayList<>());
			documentToBeWrittenToCollection.add(item);
		}
		for (final List<Object> pendingObjects : pendingItemsByCollection.values()) {
			new MongoCommand(MirrorOperation.INSERT, partitionId, pendingObjects) {
				@Override
				protected void execute(Document... documents) {
					DocumentCollection documentCollection = getDocumentCollection(pendingObjects.get(0));
					documentCollection.insertAll(documents);
				}
			}.execute(pendingObjects.toArray());
		}
	}

	private DocumentCollection getDocumentCollection(Object item) {
		return this.mirror.getDocumentCollection(item.getClass());
	}

	abstract class MongoCommand {

		private final MirrorOperation operation;
		private final Integer partitionId;
		private final Object[] objects;

		public MongoCommand(MirrorOperation operation, @Nullable Integer partitionId, Object... objects) {
			this.operation = operation;
			this.partitionId = partitionId;
			this.objects = objects;
		}

		final void execute(Object... items) {
			try {
				Document[] documents = new Document[items.length];
				for (int i = 0; i < documents.length; i++) {
					Document versionedDocument = MirroredObjectWriter.this.mirror.toVersionedDocument(items[i], partitionId);
					MirroredObjectWriter.this.mirror.getPreWriteProcessing(items[i].getClass()).preWrite(versionedDocument);
					documents[i] = versionedDocument;
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

		protected abstract void execute(Document... documents);

	}

}
