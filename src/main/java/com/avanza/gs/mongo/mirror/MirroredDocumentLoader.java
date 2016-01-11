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

import static java.util.stream.Collectors.toList;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.gs.mongo.util.NamedThreadFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
/**
 * Implementation note: multithreaded patching to increase throughput.
 * 
 * @author Elias Lindholm (elilin), Kristoffer Erlandsson, Andreas Skoog
 */
final class MirroredDocumentLoader<T> {
	
	private static final int CONVERSION_TIMEOUT_MINUTES = 60;
	private static final int NUM_THREADS = 15;
	private final Logger log = LoggerFactory.getLogger(getClass());
	private final List<PatchedDocument> pendingPatchedDocuments = new LinkedList<>();
	private final List<T> loadedObjects = new LinkedList<>();

	private final MirroredDocument<T> document;
	private final DocumentCollection documentCollection;
	private final SpaceObjectFilter<T> spaceObjectFilter;
	private final Logger LOGGER = LoggerFactory.getLogger(MirroredDocumentLoader.class);
	private final DocumentConverter documentConverter;
	private final AtomicLong numLoadedObjects = new AtomicLong();

	MirroredDocumentLoader(DocumentCollection documentCollection, DocumentConverter documentConverter, MirroredDocument<T> mirroredDocument, SpaceObjectFilter<T> spaceObjectFilter) {
		this.documentConverter = documentConverter;
		this.spaceObjectFilter = spaceObjectFilter;
		this.documentCollection = documentCollection;
		this.document = mirroredDocument;
	}

	void loadAllObjects() {
		long startTime = System.currentTimeMillis();
		log.info("Begin loadAllObjects for {}", document.getCollectionName());
		Iterable<DBObject> allObjects = loadDocuments();
		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS, new NamedThreadFactory("MirroredDocumentLoaderWorker"));
		List<Future<Optional<DocumentWrapper<T>>>> futures = 
				StreamSupport.stream(allObjects.spliterator(), false)
							 .map(x -> executor.submit(() -> tryLoadAndPatch(x)))
							 .collect(toList());
		executor.shutdown();
		awaitTermination(executor);
		futures.stream()
			   .map(this::getFutureResult)
			   .filter(Optional::isPresent)
			   .map(Optional::get)
			   .forEach(this::addToLists);
		log.info("loadAllObjects for {} finished. {} objects were loaded in {} seconds", document.getCollectionName(),
				loadedObjects.size(), ((System.currentTimeMillis() - startTime) / 1000d));
	}

	private Iterable<DBObject> loadDocuments() {
		if (document.loadDocumentsRouted()) {
			return documentCollection.findAll(spaceObjectFilter);
		}
		return documentCollection.findAll();
	}
	

	private Optional<DocumentWrapper<T>> getFutureResult(Future<Optional<DocumentWrapper<T>>> future) {
		try {
			return future.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void addToLists(DocumentWrapper<T> documentWrapper) {
		loadedObjects.add(documentWrapper.getDocument());
		documentWrapper.getPatchedDocument().ifPresent(pendingPatchedDocuments::add);
	}

	private void awaitTermination(ExecutorService executor) {
		try {
			executor.awaitTermination(CONVERSION_TIMEOUT_MINUTES, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while waiting for conversions", e);
		}
	}
	
	private Optional<DocumentWrapper<T>> tryLoadAndPatch(DBObject object) {
		try {
			Optional<DocumentWrapper<T>> result = loadAndPatch(new BasicDBObject(object.toMap()));
			long loaded = numLoadedObjects.incrementAndGet();
			if (loaded % 10000 == 0) {
				log.info("Status: loaded {} records for collection {}", loaded, document.getCollectionName());
			}
			return result;
		} catch (RuntimeException e) {
			LOGGER.error("Unable to load dbObject=" + object, e);
			throw e;
		}
	}

	Iterable<T> createPatchingIterable() {
		return () -> {
			return new PatchingIterator<>(loadDocuments().iterator(), document, documentConverter, spaceObjectFilter);
		};
	}

	void loadById(Object id) {
		BasicDBObject document = findById(id);
		if (document == null) {
			return;
		}
		loadAndPatchSingleDocument(document);
		if (loadedObjects.isEmpty()) {
			throw new IllegalArgumentException("Space object not accepted by filter (id=" + id + ")");
		}
	}
	
	private void loadAndPatchSingleDocument(BasicDBObject document) {
		Optional<DocumentWrapper<T>> wrapper = loadAndPatch(document);
		wrapper.ifPresent(this::addToLists);
	}

	void loadByQuery(T template) {
		Iterable<DBObject> documents = documentCollection.findByQuery(documentConverter.toQuery(template));
		StreamSupport.stream(documents.spliterator(), false)
			.map(BasicDBObject.class::cast)
			.forEach(this::loadAndPatchSingleDocument);
	}

	private BasicDBObject findById(Object id) {
		final Object convertedId = documentConverter.convertToMongoObject(id);
		final DBObject dbObject = documentCollection.findById(convertedId);
		return dbObject != null ? new BasicDBObject(dbObject.toMap()) : null;
	}

	private Optional<DocumentWrapper<T>> loadAndPatch(BasicDBObject dbObject) {
		BasicDBObject currentVersion = dbObject;
		boolean patched = false;
		if (this.document.requiresPatching(dbObject)) {
			patched = true;
			try {
				currentVersion = this.document.patch(dbObject);
			} catch (RuntimeException e) {
				LOGGER.error("Patch of document failed! document=" + document + "dbObject=" + dbObject, e);
				throw e;
			}
		}
		T mirroredObject = documentConverter.convert(document.getMirroredType(), currentVersion);
		if (!spaceObjectFilter.accept(mirroredObject)) {
			return Optional.empty();
		}
		if (patched) {
			return Optional.of(new DocumentWrapper<T>(postProcess(mirroredObject), Optional.of(new PatchedDocument(dbObject, currentVersion))));
		}
		return Optional.of(new DocumentWrapper<T>(postProcess(mirroredObject), Optional.empty()));
	}

	private T postProcess(T mirroredObject) {
		if (mirroredObject instanceof ReloadableSpaceObject) {
			ReloadableSpaceObjectUtil.markReloaded((ReloadableSpaceObject) mirroredObject);
		}
		return mirroredObject;
	}

	List<PatchedDocument> getPendingPatchedDocuments() {
		return pendingPatchedDocuments;
	}

	List<T> getLoadedSpaceObjects() {
		return this.loadedObjects;
	}
	
	private static class DocumentWrapper<T> {
		private final T document;
		private final Optional<PatchedDocument> patchedDocument;

		public DocumentWrapper(T document, Optional<PatchedDocument> patchedDocument) {
			this.document = document;
			this.patchedDocument = patchedDocument;
		}

		public T getDocument() {
			return document;
		}

		public Optional<PatchedDocument> getPatchedDocument() {
			return patchedDocument;
		}
		
	}

}