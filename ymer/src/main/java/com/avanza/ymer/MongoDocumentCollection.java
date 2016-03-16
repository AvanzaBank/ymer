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

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MongoDocumentCollection implements DocumentCollection {

	private final DBCollection dbCollection;

	public MongoDocumentCollection(DBCollection dbCollection) {
		this.dbCollection = Objects.requireNonNull(dbCollection);
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
	public void replace(BasicDBObject oldVersion, BasicDBObject newVersion) {
		if (!oldVersion.get("_id").equals(newVersion.get("_id"))) {
			dbCollection.insert(newVersion);
            dbCollection.remove(new BasicDBObject("_id", oldVersion.get("_id")));
        } else {
            dbCollection.update(new BasicDBObject("_id", oldVersion.get("_id")), newVersion);
        }
	}

	@Override
	public void update(BasicDBObject newVersion) {
        dbCollection.save(newVersion);
	}

	@Override
	public void insert(BasicDBObject dbObject) {
        try {
			dbCollection.insert(dbObject);
		} catch (DuplicateKeyException e) {
			throw new DuplicateDocumentKeyException(e.getMessage());
		}
	}

	@Override
	public void delete(BasicDBObject dbObject) {
        dbCollection.remove(dbObject);
	}

	@Override
	public void insertAll(BasicDBObject... dbObjects) {
        dbCollection.insert(dbObjects); // TODO: test for this method
	}

}