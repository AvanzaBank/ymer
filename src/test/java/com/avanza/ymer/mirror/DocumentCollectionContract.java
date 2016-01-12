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
/**
 *
 */
package com.avanza.ymer.mirror;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static com.avanza.ymer.mirror.Iterables.*;

import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Elias Lindholm (elilin)
 *
 */
public abstract class DocumentCollectionContract {

	private DocumentCollection documentCollection;

	@Before
	public final void before() {
		this.documentCollection = createEmptyCollection();
	}

	protected abstract DocumentCollection createEmptyCollection();

	@Test
	public void insertAddsAnElement() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);
		Iterator<DBObject> allDocs = documentCollection.findAll().iterator();
		assertEquals(d1, allDocs.next());
		assertFalse(allDocs.hasNext());
	}
	
	@Test
	public void insertExistingDocumentDoesNothing() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);

		BasicDBObject copy = (BasicDBObject) d1.copy();
		copy.put("count", 22); // copy has another count
		try {
			documentCollection.insert(copy);
			fail("Expected exeption of type: " + DuplicateDocumentKeyException.class.getName());
		} catch (DuplicateDocumentKeyException e) {
			// Expected 
		}
		// Still only one element in the collection
		assertEquals(1, sizeOf(documentCollection.findAll()));
		
		// the original version did not change on insert
		assertEquals(d1, documentCollection.findAll().iterator().next());
	}

	@Test
	public void insertGeneratesIdIfNotSet() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("count", 21);
		assertNull(d1.get("_id"));

		documentCollection.insert(d1);

		assertNotNull("_id should be generated on insert", d1.get("_id"));
		assertEquals(1, sizeOf(documentCollection.findAll()));
		assertEquals(d1, documentCollection.findAll().iterator().next());
	}

	@Test
	public void replaceUpdatesAnExistingObjectIfIdHasNotChanged() throws Exception {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("_id", "id_1");
		dbObject.put("count", 21);

		documentCollection.insert(dbObject);

		BasicDBObject newVersion = (BasicDBObject) dbObject.copy();
		newVersion.put("count", 22);

		documentCollection.replace(dbObject, newVersion);

		assertEquals(1, sizeOf(documentCollection.findAll()));
		BasicDBObject dbVersion = (BasicDBObject) documentCollection.findAll().iterator().next();
		assertEquals(22, dbVersion.get("count"));
	}

	@Test
	public void replaceWithNewIdRemovesOldDocumentAndInsertsNew() throws Exception {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("_id", "id_1");
		dbObject.put("count", 21);

		documentCollection.insert(dbObject);

		BasicDBObject newVersion = (BasicDBObject) dbObject.copy();
		newVersion.put("_id", "id_2");

		documentCollection.replace(dbObject, newVersion);

		assertEquals(1, sizeOf(documentCollection.findAll()));
		BasicDBObject dbVersion = (BasicDBObject) documentCollection.findAll().iterator().next();
		assertEquals(21, dbVersion.get("count"));
		assertEquals("id_2", dbVersion.get("_id"));
	}

	@Test
	public void findAllReturnsAllDocuments() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		BasicDBObject d2 = new BasicDBObject();
		d2.put("_id", "id_2");
		d2.put("count", 55);

		documentCollection.insert(d1);
		documentCollection.insert(d2);

		List<DBObject> all = newArrayList(documentCollection.findAll());
		assertEquals(2, all.size());
		assertEquals(d1, firstElementWithId(all, "id_1"));
		assertEquals(d2, firstElementWithId(all, "id_2"));
	}

	@Test
	public void findByIdReturnsAGivenDocument() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		BasicDBObject d2 = new BasicDBObject();
		d2.put("_id", "id_2");
		d2.put("count", 55);

		documentCollection.insert(d1);
		documentCollection.insert(d2);

		assertEquals(d1, documentCollection.findById("id_1"));
		assertEquals(d2, documentCollection.findById("id_2"));
	}

	@Test
	public void findByIdReturnsNullIfDocumentDoesNotExists() throws Exception {
		assertNull(documentCollection.findById("id_1"));
	}

	@Test
	public void deleteOnlyRequiresIdFieldToBeSet() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		BasicDBObject d2 = new BasicDBObject();
		d2.put("_id", "id_2");
		d2.put("count", 55);

		BasicDBObject d1Template = new BasicDBObject();
		d1Template.put("_id", "id_1");

		documentCollection.insert(d1);
		documentCollection.insert(d2);
		assertEquals(2, sizeOf(documentCollection.findAll()));
		documentCollection.delete(d1Template);
		assertEquals(1, sizeOf(documentCollection.findAll()));
		assertEquals(d2, documentCollection.findAll().iterator().next());
	}

	@Test
	public void deleteNonExistingDocumentDoesNothing() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		assertEquals(0, sizeOf(documentCollection.findAll()));
		documentCollection.delete(d1);
		assertEquals(0, sizeOf(documentCollection.findAll()));
	}

	@Test
	public void deleteDoesFullTemplateMatchIfAnotherElementThanIdFieldIsSet() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);
		assertEquals(1, sizeOf(documentCollection.findAll()));

		BasicDBObject d2 = new BasicDBObject();
		d2.put("_id", d1.get("_id"));
		d2.put("count", 22);

		documentCollection.delete(d2);
		assertEquals(1, sizeOf(documentCollection.findAll()));
	}

	@Test
	public void updateReplacesAnExistingDocument() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);

		BasicDBObject updated = (BasicDBObject) d1.copy();
		updated.put("count", 22); // copy has another count
		documentCollection.update(updated);

		// Still only one element in the collection
		assertEquals(1, documentCollection.findAll().count());

		// the original version is replace by the update
		assertEquals(updated, documentCollection.findAll().iterator().next());
	}

	@Test
	public void updateInsertAGivenDocumentIfItDoesNotExists() throws Exception {
		BasicDBObject d1 = new BasicDBObject();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.update(d1);

		assertEquals(1, sizeOf(documentCollection.findAll()));
		assertEquals(d1, documentCollection.findAll().iterator().next());
	}

	private DBObject firstElementWithId(List<DBObject> all, final String id) {
		return all.stream().filter(input -> input.get("_id").equals(id)).findFirst().get();
	}


}
