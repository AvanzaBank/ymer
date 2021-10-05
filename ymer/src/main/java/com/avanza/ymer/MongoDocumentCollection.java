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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MongoDocumentCollection implements DocumentCollection {

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
			return StreamSupport.stream(collection.find(mongoPartitionFilter.toBson()).spliterator(), false);
		}
		return findAll();
	}

	@Override
	public Stream<Document> findAll() {
		return StreamSupport.stream(collection.find().spliterator(), false);
	}

	@Override
	public Document findById(Object id) {
		return collection.find(new Document("_id", id)).first();
	}

	@Override
	public Stream<Document> findByQuery(Query query) {
		return StreamSupport.stream(collection.find(query.getQueryObject()).spliterator(), false);
	}

	@Override
	public Stream<Document> findByTemplate(Document template) {
		return StreamSupport.stream(collection.find(template).spliterator(), false);
	}

	@Override
	public void replace(Document oldVersion, Document newVersion) {
		idValidator.validateHasIdField("replace", newVersion);
		if (!Objects.equals(oldVersion.get("_id"), newVersion.get("_id"))) {
			insert(newVersion);
			DeleteResult deleteResult = collection.deleteOne(new Document("_id", oldVersion.get("_id")));
			idValidator.validateDeletedExistingDocument("replace", deleteResult, oldVersion);
		} else {
			UpdateResult updateResult = collection.replaceOne(Filters.eq("_id", oldVersion.get("_id")),
					newVersion);
			idValidator.validateUpdatedExistingDocument("replace", updateResult, oldVersion);
		}
	}

	@Override
	public void update(Document newVersion) {
		idValidator.validateHasIdField("update", newVersion);
		UpdateResult updateResult = collection.replaceOne(Filters.eq("_id", newVersion.get("_id")),
				newVersion,
				new ReplaceOptions().upsert(true));
		idValidator.validateUpdatedExistingDocument("update", updateResult, newVersion);
	}

	@Override
	public void insert(Document document) {
		idValidator.validateHasIdField("insert", document);
		try {
			collection.insertOne(document);
		} catch (MongoWriteException e) {
			if(e.getMessage().contains("E11000")) { // duplicate key error
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
}
