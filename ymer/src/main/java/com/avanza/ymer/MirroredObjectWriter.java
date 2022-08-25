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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MirroredObjectWriter {

	private final SpaceMirrorContext mirror;
	private final DocumentWriteExceptionHandler exceptionHandler;
	private final MirroredObjectFilterer mirroredObjectFilterer;

	MirroredObjectWriter(SpaceMirrorContext mirror,
			DocumentWriteExceptionHandler exceptionHandler,
			MirroredObjectFilterer mirroredObjectFilterer) {
		this.mirror = Objects.requireNonNull(mirror);
		this.exceptionHandler = Objects.requireNonNull(exceptionHandler);;
		this.mirroredObjectFilterer = Objects.requireNonNull(mirroredObjectFilterer);
	}

	public void executeBulk(InstanceMetadata metadata, OperationsBatchData batch) {
		List<Object> pendingWrites = new ArrayList<>();
		for (DataSyncOperation bulkItem : mirroredObjectFilterer.filterSpaceObjects(batch.getBatchDataItems())) {
			switch (bulkItem.getDataSyncOperationType()) {
				case WRITE:
					pendingWrites.add(bulkItem.getDataAsObject());
					break;
				case UPDATE:
				case PARTIAL_UPDATE:
					insertAll(metadata, pendingWrites);
					pendingWrites = new ArrayList<>();
					update(metadata, bulkItem.getDataAsObject());
					break;
				case REMOVE:
					insertAll(metadata, pendingWrites);
					pendingWrites = new ArrayList<>();
					remove(metadata, bulkItem.getDataAsObject());
					break;
				default:
					throw new UnsupportedOperationException("Bulkoperation " + bulkItem.getDataSyncOperationType() + " is not supported");
			}
		}
		insertAll(metadata, pendingWrites);
	}

	private void remove(InstanceMetadata metadata, final Object item) {
		MongoCommand mongoCommand = new MongoCommand(MirrorOperation.REMOVE, metadata, item) {
			@Override
			protected void execute(Document... documents) {
				Document id = new Document();
				id.put("_id", documents[0].get("_id"));
				getDocumentCollection(item).delete(id);
			}

		};
		mongoCommand.execute(item);
	}

	private void update(InstanceMetadata metadata, final Object item) {
		new MongoCommand(MirrorOperation.UPDATE, metadata, item) {
			@Override
			protected void execute(Document... documents) {
				getDocumentCollection(item).update(documents[0]);
			}
		}.execute(item);
	}

	private void insertAll(InstanceMetadata metadata, List<Object> items) {
		Map<String, List<Object>> pendingItemsByCollection = new HashMap<>();
		for (Object item : items) {
			String collectionName = this.mirror.getCollectionName(item.getClass());
			List<Object> documentToBeWrittenToCollection =
					pendingItemsByCollection.computeIfAbsent(collectionName, k -> new ArrayList<>());
			documentToBeWrittenToCollection.add(item);
		}
		for (final List<Object> pendingObjects : pendingItemsByCollection.values()) {
			new MongoCommand(MirrorOperation.INSERT, metadata, pendingObjects) {
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
		private final InstanceMetadata metadata;
		private final Object[] objects;

		public MongoCommand(MirrorOperation operation, InstanceMetadata metadata, Object... objects) {
			this.operation = operation;
			this.metadata = metadata;
			this.objects = objects;
		}

		final void execute(Object... items) {
			try {
				Document[] documents = new Document[items.length];
				for (int i = 0; i < documents.length; i++) {
					Document versionedDocument = MirroredObjectWriter.this.mirror.toVersionedDocument(items[i], metadata);
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
			Map<String, List<Object>> objectsPerType = Stream.of(this.objects)
					.collect(Collectors.groupingBy(o -> o.getClass().getSimpleName()));
			exceptionHandler.handleException(exception,
					"Operation: " + operation + ", objects: " + objectsPerType);
		}

		protected abstract void execute(Document... documents);

	}

}
