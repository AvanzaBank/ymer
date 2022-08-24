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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.springframework.data.domain.Sort.Direction.ASC;
import static org.springframework.data.domain.Sort.Direction.DESC;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
class FakeDocumentCollection implements DocumentCollection {

	private final ConcurrentLinkedQueue<Document> collection = new ConcurrentLinkedQueue<>();
	private final Set<IndexInfo> indexes = ConcurrentHashMap.newKeySet();
	private final AtomicInteger idGenerator = new AtomicInteger(0);

	FakeDocumentCollection() {
		indexes.add(new IndexInfo(singletonList(IndexField.create("_id", ASC)), "_id_", false, false, ""));
	}

	@Override
	public Stream<Document> findAll(SpaceObjectFilter<?> objectFilter)  {
		return new ArrayList<>(collection).stream();
	}

	@Override
	public Stream<Document> findAll()  {
		return new ArrayList<>(collection).stream();
	}

	@Override
	public void replace(Document oldVersion, Document newVersion) {
		// Note that the Iterator of the list associated with the given collectionName may reflect changes to the
		// underlying list. This behavior is similar to a database cursor who may returned elements
		// that are inserted/updated after the cursor is created.
		collection.remove(oldVersion);
		collection.add(newVersion);
	}

	@Override
	public void update(Document newVersion) {
		Iterator<Document> it = collection.iterator();
		while (it.hasNext()) {
			Document document = it.next();
			if (document.get("_id").equals(newVersion.get("_id"))) {
				it.remove();
				collection.add(newVersion);
				return;
			}
		}
		// No object found, do insert
		insert(newVersion);
	}

	@Override
	public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> bulkChanges) {
		for (WriteModel<? extends Document> bulkChange : bulkChanges) {
			if (bulkChange instanceof InsertOneModel) {
				InsertOneModel<Document> model = (InsertOneModel<Document>) bulkChange;
				insert(model.getDocument());
			}
			if (bulkChange instanceof UpdateOneModel) {
				ReplaceOneModel<Document> model = (ReplaceOneModel<Document>) bulkChange;
				update(model.getReplacement());
			}
			if (bulkChange instanceof DeleteOneModel) {
				DeleteOneModel<Document> model = (DeleteOneModel<Document>) bulkChange;
			}
		}

		return new BulkWriteResult() {
			@Override
			public boolean wasAcknowledged() {
				return false;
			}

			@Override
			public int getInsertedCount() {
				return 0;
			}

			@Override
			public int getMatchedCount() {
				return 0;
			}

			@Override
			public int getDeletedCount() {
				return 0;
			}

			@Override
			public int getModifiedCount() {
				return 0;
			}

			@Override
			public List<BulkWriteInsert> getInserts() {
				return null;
			}

			@Override
			public List<BulkWriteUpsert> getUpserts() {
				return null;
			}
		};
	}

	@Override
	public void bulkWrite(Consumer<BulkWriter> bulkWriter) {
		bulkWriter.accept(new BulkWriter() {
			@Override
			public void updatePartialByIds(Set<Object> ids, Map<String, Object> fieldsToSet) {
				ids.stream()
						.map(document -> findById(document))
						.filter(Objects::nonNull)
						.forEach(it -> it.putAll(fieldsToSet));
			}

			@Override
			public void unsetFieldsPartialByIds(Set<Object> ids, Set<String> fieldsToUnset) {
				ids.stream()
						.map(document -> findById(document))
						.filter(Objects::nonNull)
						.forEach(it -> fieldsToUnset.forEach(it::remove));
			}
		});
	}

	@Override
	public void insert(Document document) {
		for (Document doc : collection) {
			if (doc.get("_id").equals(document.get("_id"))) {
				throw new DuplicateDocumentKeyException("_id: " + document.get("_id"));
			}
		}
		if (document.get("_id") == null) {
			document.put("_id", "testid_" + idGenerator.incrementAndGet());
		}
		collection.add(document);
	}

	@Override
	public void delete(Document document) {
		Document doc = new Document();
		doc.put("_id", document.get("_id"));
		if (doc.equals(document)) {
			removeById(doc);
		} else {
			removeByTemplate(document);
		}
	}

	private void removeByTemplate(Document document) {
		Iterator<Document> it = collection.iterator();
		while (it.hasNext()) {
			Document next = it.next();
			if (next.equals(document)) {
				it.remove();
				return;
			}
		}
	}

	private void removeById(Document document) {
		Iterator<Document> it = collection.iterator();
		while (it.hasNext()) {
			Document next = it.next();
			if (next.get("_id").equals(document.get("_id"))) {
				it.remove();
				return;
			}
		}
	}

	@Override
	public void insertAll(Document... documents) {
		for (Document document : documents) {
			insert(document);
		}
	}

	@Override
	public Document findById(Object id) {
		for (Document next : collection) {
			if (next.get("_id").equals(id)) {
				return next;
			}
		}
		return null;
	}

	@Override
	public Stream<Document> findByQuery(Query query) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<Document> findByTemplate(Document template) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stream<IndexInfo> getIndexes() {
		return indexes.stream();
	}

	@Override
	public void dropIndex(String name) {
		indexes.removeIf(index -> Objects.equals(index.getName(), name));
	}

	@Override
	public void createIndex(Document keys, IndexOptions indexOptions) {
		List<IndexField> indexFields = keys.entrySet().stream().flatMap(entry -> toIndexFieldOrEmpty(entry.getKey(), entry.getValue())).collect(toList());
		IndexInfo index = new IndexInfo(indexFields, indexOptions.getName(), indexOptions.isUnique(), indexOptions.isSparse(), indexOptions.getDefaultLanguage());
		indexes.add(index);
	}

	private Stream<IndexField> toIndexFieldOrEmpty(String key, @Nullable Object value) {
		return toDirection(value)
				.map(direction -> IndexField.create(key, direction))
				.stream();
	}

	private Optional<Sort.Direction> toDirection(@Nullable Object direction) {
		if (direction == null) {
			return Optional.empty();
		} else {
			switch (direction.toString()) {
				case "1":
					return Optional.of(ASC);
				case "-1":
					return Optional.of(DESC);
				default:
					return Optional.empty();
			}
		}
	}
}
