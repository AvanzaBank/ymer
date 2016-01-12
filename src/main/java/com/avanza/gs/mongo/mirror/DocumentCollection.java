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
package com.avanza.gs.mongo.mirror;

import java.util.stream.Stream;

import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
/**
 * Abstraction over {@link DBCollection}. <p>
 *
 * @author Elias Lindholm (elilin)
 *
 */
interface DocumentCollection {
	
	/**
	 * Reads all documents from the underlying mongo collection. <p>
	 */
	Stream<DBObject> findAll();
	
	Stream<DBObject> findAll(SpaceObjectFilter<?> objectFilter);

	/**
	 * Returns a document with a given id. <p>
	 *
	 */
	DBObject findById(Object id);

	Stream<DBObject> findByQuery(Query query);

	/**
	 * Replaces a given document in the underlying mongo collection with a new
	 * document. <p>
	 *
	 * This operation differs from update in that the id of the newVersion may
	 * have changed from the id of the oldVersion.
	 *
	 */
	void replace(BasicDBObject oldVersion, BasicDBObject newVersion);

	/**
	 * Updates a given object (identified by id) in the underlying mongo collection. <p>
	 */
	void update(BasicDBObject newVersion);

	/**
	 * Inserts the given object into the underlying mongo collection. <p>
	 */
	void insert(BasicDBObject dbObject);

	void delete(BasicDBObject dbObject);

	/**
	 * Inserts all documents in a single batch to the underlying mongo collection. <p>
	 */
	void insertAll(BasicDBObject... dbObjects);

}