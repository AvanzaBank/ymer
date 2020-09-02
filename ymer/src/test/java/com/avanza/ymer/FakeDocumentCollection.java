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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
class FakeDocumentCollection implements DocumentCollection {

	private final ConcurrentLinkedQueue<Document> collection = new ConcurrentLinkedQueue<>();
	private final AtomicInteger idGenerator = new AtomicInteger(0);

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
}
