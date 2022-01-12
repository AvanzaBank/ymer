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

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static java.util.Spliterators.spliteratorUnknownSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MongoDocumentCollection implements DocumentCollection {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private final MongoCollection<Document> collection;
	private final IdValidator idValidator;

	interface IdValidator {
		void validateHasIdField(String operation, Document obj);
		void validateUpdatedExistingDocument(String operation, UpdateResult result, Document obj);
		void validateDeletedExistingDocument(String operation, DeleteResult result, Document obj);
	}

	public MongoDocumentCollection(MongoCollection<Document> collection) {
		this(collection, new IdValidatorImpl(collection.getNamespace().getCollectionName()));
	}

	MongoDocumentCollection(MongoCollection<Document> collection, IdValidator idValidator) {
		this.collection = Objects.requireNonNull(collection);
		this.idValidator = Objects.requireNonNull(idValidator);
	}

	@Override
	public Stream<Document> findAll(SpaceObjectFilter<?> objectFilter) {
		if (MongoPartitionFilter.canCreateFrom(objectFilter)) {
			MongoPartitionFilter mongoPartitionFilter = MongoPartitionFilter.createBsonFilter(objectFilter);
			return toStream(collection.find(mongoPartitionFilter.toBson()));
		}
		return findAll();
	}

	@Override
	public Stream<Document> findAll() {
		return toStream(collection.find());
	}


	@Override
	public Document findById(Object id) {
		return collection.find(Filters.eq(id)).first();
	}

	@Override
	public Stream<Document> findByQuery(Query query) {
		FindIterable<Document> iterable = collection.find(query.getQueryObject());
		Document fieldsObject = query.getFieldsObject();
		if (!fieldsObject.isEmpty()) {
			iterable = iterable.projection(fieldsObject);
		}
		Integer batchSize = query.getMeta().getCursorBatchSize();
		if (batchSize != null) {
			iterable = iterable.batchSize(batchSize);
		}

		return toStream(iterable);
	}

	@Override
	public long countByQuery(Query query) {
		return collection.countDocuments(query.getQueryObject());
	}

	@Override
	public Stream<Document> findByTemplate(Document template) {
		return toStream(collection.find(template));
	}

	@Override
	public void replace(Document oldVersion, Document newVersion) {
		idValidator.validateHasIdField("replace", newVersion);
		if (!Objects.equals(oldVersion.get("_id"), newVersion.get("_id"))) {
			insert(newVersion);
			DeleteResult deleteResult = collection.deleteOne(Filters.eq(oldVersion.get("_id")));
			idValidator.validateDeletedExistingDocument("replace", deleteResult, oldVersion);
		} else {
			UpdateResult updateResult = collection.replaceOne(Filters.eq(oldVersion.get("_id")),
					newVersion);
			idValidator.validateUpdatedExistingDocument("replace", updateResult, oldVersion);
		}
	}

	@Override
	public void update(Document newVersion) {
		idValidator.validateHasIdField("update", newVersion);
		UpdateResult updateResult = collection.replaceOne(Filters.eq(newVersion.get("_id")),
				newVersion,
				new ReplaceOptions().upsert(true));
		idValidator.validateUpdatedExistingDocument("update", updateResult, newVersion);
	}

	@Override
	public void bulkWrite(Consumer<BulkWriter> bulkWriter) {
		List<WriteModel<Document>> writeModels = new ArrayList<>();
		bulkWriter.accept(new BulkWriter() {
			@Override
			public void updatePartialByIds(Set<Object> ids, Map<String, Object> fieldsToSet) {
				Bson updates = toUpdates(fieldsToSet);
				addUpdates(ids, updates);
			}

			private void addUpdates(Set<Object> ids, Bson updates) {
				if (ids.isEmpty()) {
					log.warn("Skipping updates because no ids provided");
				} else if (updates == null) {
					log.warn("Skipping updates because no fields to update provided");
				} else {
					Bson filter = Filters.in("_id", ids);
					UpdateManyModel<Document> updateManyModel = new UpdateManyModel<>(filter, updates);
					writeModels.add(updateManyModel);
				}
			}

			@Override
			public void unsetFieldsPartialByIds(Set<Object> ids, Set<String> fieldsToUnset) {
				Bson updates = toFieldDeletes(fieldsToUnset);
				addUpdates(ids, updates);
			}
		});
		if (writeModels.isEmpty()) {
			log.debug("Skipping bulkWrite because no operations provided");
		} else {
			collection.bulkWrite(writeModels, new BulkWriteOptions().ordered(false));
		}
	}

	@Override
	public void insert(Document document) {
		idValidator.validateHasIdField("insert", document);
		try {
			collection.insertOne(document);
		} catch (MongoWriteException e) {
			if(e.getError().getCategory() == DUPLICATE_KEY) {
				throw new DuplicateDocumentKeyException(e.getMessage());
			} else {
				throw e;
			}
		}
	}

	@Override
	public void delete(Document document) {
		idValidator.validateHasIdField("delete", document);
		DeleteResult deleteResult = collection.deleteOne(document);
		idValidator.validateDeletedExistingDocument("delete", deleteResult, document);
	}

	@Override
	public void insertAll(Document... documents) {
		if (documents.length != 0) {
			idValidator.validateHasIdField("insert", documents[0]);
		}
		collection.insertMany(Arrays.asList(documents)); // TODO: test for this method
	}

	@Override
	public Stream<IndexInfo> getIndexes() {
		return toStream(collection.listIndexes().map(IndexInfo::indexInfoOf));
	}

	@Override
	public void dropIndex(String name) {
		collection.dropIndex(name);
	}

	@Override
	public void createIndex(Document keys, IndexOptions indexOptions) {
		collection.createIndex(keys, indexOptions);
	}

	@Nullable
	private static Bson toUpdates(Map<String, Object> fieldsToSet) {
		return fieldsToSet.entrySet().stream()
				.map(entry -> Updates.set(entry.getKey(), entry.getValue()))
				.reduce(Updates::combine)
				.orElse(null);
	}

	@Nullable
	private static Bson toFieldDeletes(Set<String> fieldsToUnset) {
		return fieldsToUnset.stream()
				.map(Updates::unset)
				.reduce(Updates::combine)
				.orElse(null);
	}

	private static <T> Stream<T> toStream(MongoIterable<T> mongoIterable) {
		MongoCursor<T> iterator = mongoIterable.iterator();
		return StreamSupport.stream(spliteratorUnknownSize(iterator, 0), false)
				.onClose(iterator::close);
	}
}
