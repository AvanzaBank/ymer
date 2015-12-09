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
package se.avanzabank.mongodb.support.mirror;

import java.util.Optional;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.springframework.data.mongodb.core.query.Query;
import se.avanzabank.core.util.assertions.Require;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class MongoDocumentCollection implements DocumentCollection {

	private final DBCollection dbCollection;

	public MongoDocumentCollection(DBCollection dbCollection) {
		this.dbCollection = Require.notNull(dbCollection);
	}

	@Override
	public Iterable<DBObject> findAll(Optional<SpaceObjectFilter<?>> objectFilter) {
		if (objectFilter.isPresent() && MongoPartitionFilter.canCreateFrom(objectFilter.get())) {
			MongoPartitionFilter mongoPartitionFilter = MongoPartitionFilter.create(objectFilter.get());
			return dbCollection.find(mongoPartitionFilter.toDBObject());
		} else {
			return dbCollection.find();
		}
	}

	@Override
	public DBObject findById(Object id) {
		return dbCollection.findOne(id);
	}

	@Override
	public Iterable<DBObject> findByQuery(Query query) {
		return dbCollection.find(query.getQueryObject());
	}

	@Override
	public void replace(BasicDBObject oldVersion, BasicDBObject newVersion) {
		if (!oldVersion.get("_id").equals(newVersion.get("_id"))) {
			dbCollection.insert(newVersion);
            dbCollection.remove(oldVersion);
        } else {
            dbCollection.update(oldVersion, newVersion);
        }
	}

	@Override
	public void update(BasicDBObject newVersion) {
        dbCollection.save(newVersion);
	}

	@Override
	public void insert(BasicDBObject dbObject) {
        dbCollection.insert(dbObject);
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