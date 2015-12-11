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

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.data.mongodb.core.query.Query;
/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
class FakeDocumentCollection implements DocumentCollection {

	private final ConcurrentLinkedQueue<DBObject> collection = new ConcurrentLinkedQueue<>();
	private final AtomicInteger idGenerator = new AtomicInteger(0);

	@Override
	public Iterable<DBObject> findAll(Optional<SpaceObjectFilter<?>> filter) {
		return getDocumentCollection();
	}

	private ConcurrentLinkedQueue<DBObject> getDocumentCollection() {
		return collection;
	}

	@Override
	public void replace(BasicDBObject oldVersion, BasicDBObject newVersion) {
		// Note that the Iterator of the list associated with the given collectionName may reflect changes to the
		// underlying list. This behavior is similar to a database cursor who may returned elements
		// that are inserted/updated after the cursor is created.
		getDocumentCollection().remove(oldVersion);
		getDocumentCollection().add(newVersion);
	}

	public void addDocument(String collectionName, BasicDBObject doc) {
		getDocumentCollection().add(doc);
	}

	@Override
	public void update(BasicDBObject newVersion) {
		Iterator<DBObject> it = getDocumentCollection().iterator();
		while (it.hasNext()) {
			DBObject dbObject = it.next();
			if (dbObject.get("_id").equals(newVersion.get("_id"))) {
				it.remove();
				getDocumentCollection().add(newVersion);
				return;
			}
		}
		// No object found, do insert
		insert(newVersion);
	}

	@Override
	public void insert(BasicDBObject dbObject) {
		for (DBObject object : getDocumentCollection()) {
			if (object.get("_id").equals(dbObject.get("_id"))) {
				throw new DuplicateDocumentKeyException("_id: " + dbObject.get("_id"));
			}
		}
		if (dbObject.get("_id") == null) {
			dbObject.put("_id", "testid_" + idGenerator.incrementAndGet());
		}
		getDocumentCollection().add(dbObject);
	}

	@Override
	public void delete(BasicDBObject dbObject) {
		BasicDBObject idOBject = new BasicDBObject();
		idOBject.put("_id", dbObject.get("_id"));
		if (idOBject.equals(dbObject)) {
			removeById(idOBject);
		} else {
			removeByTemplate(dbObject);
		}

	}

	private void removeByTemplate(BasicDBObject dbObject) {
		Iterator<DBObject> it = getDocumentCollection().iterator();
		while (it.hasNext()) {
			DBObject next = it.next();
			if (next.equals(dbObject)) {
				it.remove();
				return;
			}
		}
	}

	private void removeById(BasicDBObject dbObject) {
		Iterator<DBObject> it = getDocumentCollection().iterator();
		while (it.hasNext()) {
			DBObject next = it.next();
			if (next.get("_id").equals(dbObject.get("_id"))) {
				it.remove();
				return;
			}
		}
	}

	@Override
	public void insertAll(BasicDBObject... dbObjects) {
		for (BasicDBObject dbObject : dbObjects) {
			insert(dbObject);
		}
	}

	@Override
	public DBObject findById(Object id) {
		for (DBObject next : getDocumentCollection()) {
			if (next.get("_id").equals(id)) {
				return next;
			}
		}
		return null;
	}

	@Override
	public Iterable<DBObject> findByQuery(Query query) {
		throw new UnsupportedOperationException();
	}
}