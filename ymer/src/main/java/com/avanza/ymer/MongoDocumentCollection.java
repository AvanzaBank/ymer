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

import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.WriteResult;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MongoDocumentCollection implements DocumentCollection {

	private final DBCollection dbCollection;
	private final IdValidator idValidator;

	interface IdValidator {
		void validateHasIdField(String operation, DBObject obj);
		void validateTouchedExistingDocument(String operation, WriteResult result, DBObject obj);
	}

	public MongoDocumentCollection(DBCollection dbCollection) {
		this(dbCollection, new IdValidatorImpl(dbCollection.getName()));
	}

	MongoDocumentCollection(DBCollection dbCollection, IdValidator idValidator) {
		this.dbCollection = Objects.requireNonNull(dbCollection);
		this.idValidator = Objects.requireNonNull(idValidator);
	}

	@Override
	public Stream<DBObject> findAll(SpaceObjectFilter<?> objectFilter) {
		if (MongoPartitionFilter.canCreateFrom(objectFilter)) {
			MongoPartitionFilter mongoPartitionFilter = MongoPartitionFilter.create(objectFilter);
			return StreamSupport.stream(dbCollection.find(mongoPartitionFilter.toDBObject()).spliterator(), false);
		}
		return findAll();
	}

	@Override
	public Stream<DBObject> findAll() {
		return StreamSupport.stream(dbCollection.find().spliterator(), false);
	}

	@Override
	public DBObject findById(Object id) {
		return dbCollection.findOne(id);
	}

	@Override
	public Stream<DBObject> findByQuery(Query query) {
		return  StreamSupport.stream(dbCollection.find(query.getQueryObject()).spliterator(), false);
	}

	@Override
	public Stream<DBObject> findByTemplate(BasicDBObject query) {
		return  StreamSupport.stream(dbCollection.find(query).spliterator(), false);
	}

	@Override
	public void replace(DBObject oldVersion, DBObject newVersion) {
		final WriteResult result;
		idValidator.validateHasIdField("replace", newVersion);
		if (!oldVersion.get("_id").equals(newVersion.get("_id"))) {
			dbCollection.insert(newVersion);
			result = dbCollection.remove(new BasicDBObject("_id", oldVersion.get("_id")));
        } else {
			result = dbCollection.update(new BasicDBObject("_id", oldVersion.get("_id")), newVersion);
		}
		idValidator.validateTouchedExistingDocument("replace", result, oldVersion);
	}

	@Override
	public void update(DBObject newVersion) {
		idValidator.validateHasIdField("update", newVersion);
		// ".save()" might set the id if missing, so keep a copy of the original
		final DBObject original = new BasicDBObject(newVersion.toMap());
		WriteResult result = dbCollection.save(newVersion);
		idValidator.validateTouchedExistingDocument("update", result, original);
	}

	@Override
	public void insert(DBObject dbObject) {
		idValidator.validateHasIdField("insert", dbObject);
        try {
			dbCollection.insert(dbObject);
		} catch (DuplicateKeyException e) {
			throw new DuplicateDocumentKeyException(e.getMessage());
		}
	}

	@Override
	public void delete(BasicDBObject dbObject) {
		idValidator.validateHasIdField("delete", dbObject);
		WriteResult result = dbCollection.remove(dbObject);
		idValidator.validateTouchedExistingDocument("delete", result, dbObject);
	}

	@Override
	public void insertAll(DBObject... dbObjects) {
		if (dbObjects.length != 0) {
			idValidator.validateHasIdField("insert", dbObjects[0]);
		}
        dbCollection.insert(dbObjects); // TODO: test for this method
	}

}
