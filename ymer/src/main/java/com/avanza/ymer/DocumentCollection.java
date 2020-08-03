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

import java.util.List;
import java.util.stream.Stream;

import org.bson.Document;
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

	/**
	 * Reads all documents from the underlying mongo collection. <p>
	 */
	Stream<Document> findAll2();

	Stream<DBObject> findAll(SpaceObjectFilter<?> objectFilter);

	Stream<DBObject> findByTemplate(BasicDBObject template);

	Stream<Document> findAll2(SpaceObjectFilter<?> objectFilter);

	Stream<Document> findByTemplate(Document template);

	/**
	 * Returns a document with a given id. <p>
	 *
	 */
	DBObject findById(Object id);

	Stream<DBObject> findByQuery(Query query);

	/**
	 * Returns a document with a given id. <p>
	 *
	 */
	Document findById2(Object id);

	Stream<Document> findByQuery2(Query query);

	/**
	 * Replaces a given document in the underlying mongo collection with a new
	 * document. <p>
	 *
	 * This operation differs from update in that the id of the newVersion may
	 * have changed from the id of the oldVersion.
	 *
	 */
	void replace(DBObject oldVersion, DBObject newVersion);

	/**
	 * Replaces a given document in the underlying mongo collection with a new
	 * document. <p>
	 *
	 * This operation differs from update in that the id of the newVersion may
	 * have changed from the id of the oldVersion.
	 *
	 */
	void replace(Document oldVersion, Document newVersion);

	/**
	 * Updates a given object (identified by id) in the underlying mongo collection. <p>
	 */
	void update(DBObject newVersion);

	/**
	 * Updates a given document (identified by id) in the underlying mongo collection. <p>
	 */
	void update(Document document);

	/**
	 * Inserts the given object into the underlying mongo collection. <p>
	 */
	void insert(DBObject dbObject);

	/**
	 * Inserts the given object into the underlying mongo collection. <p>
	 */
	void insert(Document dbObject);

	void delete(BasicDBObject dbObject);

	void delete(Document document);

	/**
	 * Inserts all documents in a single batch to the underlying mongo collection. <p>
	 */
	void insertAll(DBObject... dbObjects);

	/**
	 * Inserts all documents in a single batch to the underlying mongo collection. <p>
	 */
	void insertAll(Document... documents);

}