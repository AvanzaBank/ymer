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

import static com.avanza.ymer.Iterables.newArrayList;
import static com.avanza.ymer.Iterables.sizeOf;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.springframework.data.domain.Sort.Direction.ASC;

import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.mongodb.core.index.IndexField;
import org.springframework.data.mongodb.core.index.IndexInfo;

import com.mongodb.client.model.IndexOptions;

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
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);
		Iterator<Document> allDocs = documentCollection.findAll().iterator();
		assertEquals(d1, allDocs.next());
		assertFalse(allDocs.hasNext());
	}
	
	@Test
	public void insertExistingDocumentDoesNothing() throws Exception {
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);

		Document copy = new Document(d1);
		copy.put("count", 22); // copy has another count
		try {
			documentCollection.insert(copy);
			fail("Expected exception of type: " + DuplicateDocumentKeyException.class.getName());
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
		Document d1 = new Document();
		d1.put("count", 21);
		assertNull(d1.get("_id"));

		documentCollection.insert(d1);

		assertNotNull("_id should be generated on insert", d1.get("_id"));
		assertEquals(1, sizeOf(documentCollection.findAll()));
		assertEquals(d1, documentCollection.findAll().iterator().next());
	}

	@Test
	public void replaceUpdatesAnExistingObjectIfIdHasNotChanged() throws Exception {
		Document document = new Document();
		document.put("_id", "id_1");
		document.put("count", 21);

		documentCollection.insert(document);

		Document newVersion = new Document(document);
		newVersion.put("count", 22);

		documentCollection.replace(document, newVersion);

		assertEquals(1, sizeOf(documentCollection.findAll()));
		Document dbVersion = documentCollection.findAll().iterator().next();
		assertEquals(22, dbVersion.get("count"));
	}

	@Test
	public void replaceWithNewIdRemovesOldDocumentAndInsertsNew() throws Exception {
		Document document = new Document();
		document.put("_id", "id_1");
		document.put("count", 21);

		documentCollection.insert(document);

		Document newVersion = new Document(document);
		newVersion.put("_id", "id_2");

		documentCollection.replace(document, newVersion);

		assertEquals(1, sizeOf(documentCollection.findAll()));
		Document dbVersion = documentCollection.findAll().iterator().next();
		assertEquals(21, dbVersion.get("count"));
		assertEquals("id_2", dbVersion.get("_id"));
	}

	@Test
	public void findAllReturnsAllDocuments() throws Exception {
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		Document d2 = new Document();
		d2.put("_id", "id_2");
		d2.put("count", 55);

		documentCollection.insert(d1);
		documentCollection.insert(d2);

		List<Document> all = newArrayList(documentCollection.findAll());
		assertEquals(2, all.size());
		assertEquals(d1, firstElementWithId(all, "id_1"));
		assertEquals(d2, firstElementWithId(all, "id_2"));
	}

	@Test
	public void findByIdReturnsAGivenDocument() throws Exception {
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		Document d2 = new Document();
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
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		Document d2 = new Document();
		d2.put("_id", "id_2");
		d2.put("count", 55);

		Document d1Template = new Document();
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
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		assertEquals(0, sizeOf(documentCollection.findAll()));
		documentCollection.delete(d1);
		assertEquals(0, sizeOf(documentCollection.findAll()));
	}

	@Test
	public void deleteDoesFullTemplateMatchIfAnotherElementThanIdFieldIsSet() throws Exception {
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);
		assertEquals(1, sizeOf(documentCollection.findAll()));

		Document d2 = new Document();
		d2.put("_id", d1.get("_id"));
		d2.put("count", 22);

		documentCollection.delete(d2);
		assertEquals(1, sizeOf(documentCollection.findAll()));
	}

	@Test
	public void updateReplacesAnExistingDocument() throws Exception {
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.insert(d1);

		Document updated = new Document(d1);
		updated.put("count", 22); // copy has another count
		documentCollection.update(updated);

		// Still only one element in the collection
		assertEquals(1, documentCollection.findAll().count());

		// the original version is replace by the update
		assertEquals(updated, documentCollection.findAll().iterator().next());
	}

	@Test
	public void updateInsertAGivenDocumentIfItDoesNotExists() throws Exception {
		Document d1 = new Document();
		d1.put("_id", "id_1");
		d1.put("count", 21);

		documentCollection.update(d1);

		assertEquals(1, sizeOf(documentCollection.findAll()));
		assertEquals(d1, documentCollection.findAll().iterator().next());
	}

	@Test
	public void shouldReturnOnlyTheDefaultIndex() throws Exception {
		documentCollection.insert(new Document("_id", 1));
		List<IndexInfo> indexes = documentCollection.getIndexes().collect(toList());

		assertThat(indexes, hasSize(1));
		assertThat(indexes.get(0).isIndexForFields(singleton("_id")), equalTo(true));
	}

	@Test
	public void shouldCreateIndex() throws Exception {
		String fieldName = "myField";
		String indexName = "myIndex";

		documentCollection.createIndex(new Document(fieldName, 1), new IndexOptions().name(indexName));

		List<IndexInfo> indexes = documentCollection.getIndexes().filter(index -> !index.isIndexForFields(singleton("_id"))).collect(toList());
		assertThat(indexes, hasSize(1));
		assertThat(indexes.get(0).getName(), equalTo(indexName));
		assertThat(indexes.get(0).getIndexFields(), contains(IndexField.create(fieldName, ASC)));
	}

	@Test
	public void shouldDropIndex() throws Exception {
		String indexName = "myIndex";
		documentCollection.createIndex(new Document("myField", 1), new IndexOptions().name(indexName));

		documentCollection.dropIndex(indexName);

		List<IndexInfo> indexes = documentCollection.getIndexes().filter(index -> !index.isIndexForFields(singleton("_id"))).collect(toList());
		assertThat(indexes, empty());
	}

	private Document firstElementWithId(List<Document> all, final String id) {
		return all.stream().filter(input -> input.get("_id").equals(id)).findFirst().orElseThrow();
	}
}
