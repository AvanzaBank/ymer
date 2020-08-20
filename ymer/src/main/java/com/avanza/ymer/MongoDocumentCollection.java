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
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MongoDocumentCollection implements DocumentCollection {

	private final DBCollection dbCollection;
	private MongoCollection<Document> collection;

	public MongoDocumentCollection(DBCollection dbCollection) {
		this.dbCollection = Objects.requireNonNull(dbCollection);
	}

	public MongoDocumentCollection(MongoCollection<Document> collection) {
		this.collection = Objects.requireNonNull(collection);
		this.dbCollection = null;
	}

	@Override
	public Stream<DBObject> findAll(SpaceObjectFilter<?> objectFilter) {
		if (MongoPartitionFilter.canCreateFrom(objectFilter)) {
			MongoPartitionFilter mongoPartitionFilter = MongoPartitionFilter.createBsonFilter(objectFilter);
			return StreamSupport.stream(dbCollection.find(mongoPartitionFilter.toDBObject()).spliterator(), false);
		}
		return findAll();
	}

	@Override
	public Stream<Document> findAll2(SpaceObjectFilter<?> objectFilter) {
		if (MongoPartitionFilter.canCreateFrom(objectFilter)) {
			MongoPartitionFilter mongoPartitionFilter = MongoPartitionFilter.createBsonFilter(objectFilter);
			return StreamSupport.stream(collection.find(mongoPartitionFilter.toBson()).spliterator(), false);
		}
		return findAll2();
	}

	@Override
	public Stream<DBObject> findAll() {
		return StreamSupport.stream(dbCollection.find().spliterator(), false);
	}

	@Override
	public Stream<Document> findAll2() {
		return StreamSupport.stream(collection.find().spliterator(), false);
	}

	@Override
	public DBObject findById(Object id) {
		return dbCollection.findOne(id);
	}

	@Override
	public Document findById2(Object id) {
		return collection.find(new Document("_id", id)).first();
	}

	@Override
	public Stream<DBObject> findByQuery(Query query) {
		return  StreamSupport.stream(dbCollection.find(new BasicDBObject(query.getQueryObject())).spliterator(), false);
	}

	@Override
	public Stream<Document> findByQuery2(Query query) {
		return StreamSupport.stream(collection.find(query.getQueryObject()).spliterator(), false);
	}

	@Override
	public Stream<DBObject> findByTemplate(BasicDBObject query) {
		return  StreamSupport.stream(dbCollection.find(query).spliterator(), false);
	}

	@Override
	public Stream<Document> findByTemplate(Document template) {
		return StreamSupport.stream(collection.find(template).spliterator(), false);
	}

	@Override
	public void replace(DBObject oldVersion, DBObject newVersion) {
		if (!oldVersion.get("_id").equals(newVersion.get("_id"))) {
			dbCollection.insert(newVersion);
            dbCollection.remove(new BasicDBObject("_id", oldVersion.get("_id")));
        } else {
            dbCollection.update(new BasicDBObject("_id", oldVersion.get("_id")), newVersion);
        }
	}

	@Override
	public void replace(Document oldVersion, Document newVersion) {
		if (!Objects.equals(oldVersion.get("_id"),newVersion.get("_id"))) {
			collection.insertOne(newVersion);
			collection.deleteOne(new Document("_id", oldVersion.get("_id")));
		} else {
			collection.updateOne(Filters.eq("_id", oldVersion.get("_id")),
								 new Document("$set", newVersion));
		}
	}

	@Override
	public void update(DBObject newVersion) {
        dbCollection.save(newVersion);
	}

	@Override
	public void update(Document newVersion) {
		collection.updateOne(Filters.eq("_id", newVersion.get("_id")),
							 new Document("$set", newVersion),
							 new UpdateOptions().upsert(true));
	}

	@Override
	public void insert(DBObject dbObject) {
        try {
			dbCollection.insert(dbObject);
		} catch (DuplicateKeyException e) {
			throw new DuplicateDocumentKeyException(e.getMessage());
		}
	}

	@Override
	public void insert(Document document) {
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
	public void delete(BasicDBObject dbObject) {
        dbCollection.remove(dbObject);
	}

	@Override
	public void delete(Document document) {
		collection.deleteOne(document);
	}

	@Override
	public void insertAll(DBObject... dbObjects) {
        dbCollection.insert(dbObjects); // TODO: test for this method
	}

	@Override
	public void insertAll(Document... documents) {
		collection.insertMany(Arrays.asList(documents)); // TODO: test for this method
	}
}