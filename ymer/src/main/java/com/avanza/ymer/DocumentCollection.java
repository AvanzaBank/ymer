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

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.bson.Document;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.DBCollection;
import com.mongodb.client.model.IndexOptions;

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
	Stream<Document> findAll();

	Stream<Document> findAll(SpaceObjectFilter<?> objectFilter);

	Stream<Document> findByTemplate(Document template);

	/**
	 * Returns a document with a given id. <p>
	 *
	 */
	Document findById(Object id);

	Stream<Document> findByQuery(Query query);

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
	 * Updates a given document (identified by id) in the underlying mongo collection. <p>
	 */
	void update(Document document);

	/**
	 * Perform multiple write operations in bulk
	 */
	void bulkWrite(Consumer<BulkWriter> bulkWriter);

	/**
	 * Inserts the given object into the underlying mongo collection. <p>
	 */
	void insert(Document dbObject);

	void delete(Document document);

	/**
	 * Inserts all documents in a single batch to the underlying mongo collection. <p>
	 */
	void insertAll(Document... documents);

	Stream<IndexInfo> getIndexes();

	void dropIndex(String name);

	void createIndex(Document keys, IndexOptions indexOptions);

	interface BulkWriter {

		void updatePartialByIds(Set<Object> ids, Map<String, Object> fieldsToSet);

		void unsetFieldsPartialByIds(Set<Object> ids, Set<String> fieldsToUnset);

	}
}
