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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.mongodb.BasicDBObject;

import se.avanzabank.mongodb.util.Require;
/**
 * Each element loaded by the underlying iterator will be patched when fetched using next method. <p>
 * 
 * When the last element is fetched all patched documents will be written back to the document storage
 * (mongodb).
 * 
 * 
 *
 * @param <T>
 */
final class DocumentPatchDecorator<T> implements Iterator<T> {
	
	// TODO: remove or start use this class

	private final Iterator<BasicDBObject> source;
	private final DocumentConverter documentConverter;
	private final MirroredDocument<T> document;
	private final DocumentCollection documentStore;
	
	private final int partitionId;
	private final int partitionCount;
	
	private T next;
	private BasicDBObject dbObject;
	private List<PatchedDocument> pendingPatchedDocuments = new ArrayList<PatchedDocument>();
	
	public DocumentPatchDecorator(Iterator<BasicDBObject> source, 
								  DocumentConverter documentConverter, 
								  MirroredDocument<T> document, 
								  DocumentCollection documentStore,
								  int partitionId,
								  int partitionCount) {
		this.documentConverter = documentConverter;
		this.document = document;
		this.documentStore = documentStore;
		this.partitionId = partitionId;
		this.partitionCount = partitionCount;
		this.source = Require.notNull(source);
		findNext();
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}
	
	// Find next document routed to this partition.
	private void findNext() {
		while (source.hasNext()) {
			dbObject = source.next();
			if (this.document.requiresPatching(dbObject)) {
				BasicDBObject oldVersion = dbObject;
				dbObject = this.document.patch(dbObject);
				this.pendingPatchedDocuments.add(new PatchedDocument(oldVersion, dbObject));
			}
			T nextCandidate = documentConverter.convert(this.document.getMirroredType(), dbObject);
			if (isRoutedToThisPartition(nextCandidate)) {
				// Found next document
				next = nextCandidate;
				return;
			}
		}
		// No more documents, write back all patched documents to document store.
		for (PatchedDocument patchedDoc : this.pendingPatchedDocuments) {
			documentStore.replace(patchedDoc.getOldVersion(), patchedDoc.getNewVersion());
		}
		next = null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	
	}
	@Override
	public T next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		T result = next;
		findNext();
		return result;
	}
	
	private boolean isRoutedToThisPartition(T spaceType) {
		Object routingKey = getRoutingKey(spaceType);
		return routesToThisPartition(routingKey);
	}
	
	private Object getRoutingKey(T spaceType) {
		return this.document.getRoutingKey(spaceType);
	}
	
	private boolean routesToThisPartition(Object routingKey) {
		return partitionId == safeAbsoluteValue(routingKey.hashCode()) % partitionCount + 1;
	}
	
	private int safeAbsoluteValue(int value) {
	     return value == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(value);
	}

}
