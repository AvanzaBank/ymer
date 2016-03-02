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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.ymer.util.OptionalUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
/**
 * Loads mirrored objects from an external (persistent) source.
 * 
 * Implementation note: multithreaded patching to increase throughput.
 * 
 * @author Elias Lindholm (elilin), Kristoffer Erlandsson, Andreas Skoog
 */
final class MirroredObjectLoader<T> {
	private static final int NUM_THREADS = 15;
	private final Logger log = LoggerFactory.getLogger(getClass());

	private final MirroredObject<T> mirroredObject;
	private final DocumentCollection documentCollection;
	private final SpaceObjectFilter<T> spaceObjectFilter;
	private final Logger LOGGER = LoggerFactory.getLogger(MirroredObjectLoader.class);
	private final DocumentConverter documentConverter;
	private final AtomicLong numLoadedObjects = new AtomicLong();

	MirroredObjectLoader(DocumentCollection documentCollection, DocumentConverter documentConverter, MirroredObject<T> mirroredObject, SpaceObjectFilter<T> spaceObjectFilter) {
		this.documentConverter = documentConverter;
		this.spaceObjectFilter = spaceObjectFilter;
		this.documentCollection = documentCollection;
		this.mirroredObject = mirroredObject;
	}

	List<LoadedDocument<T>> loadAllObjects() {
		long startTime = System.currentTimeMillis();
		log.info("Begin loadAllObjects. targetCollection={}", mirroredObject.getCollectionName());
		ForkJoinPool forkJoinPool = new ForkJoinPool(NUM_THREADS);
		ForkJoinTask<List<LoadedDocument<T>>> loadedDocuments = forkJoinPool.submit(() -> {
			return loadDocuments().parallel()
								  .map(this::tryPatchAndConvert)
								  .flatMap(OptionalUtil::asStream)
								  .collect(Collectors.toList());
		});
		try {
			List<LoadedDocument<T>> result = loadedDocuments.get();
			log.info("loadAllObjects for {} finished. {} objects were loaded in {} seconds", mirroredObject.getCollectionName(),
					result.size(), ((System.currentTimeMillis() - startTime) / 1000d));
			return result;
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		} finally {
			forkJoinPool.shutdown();
		}
	}

	private Stream<DBObject> loadDocuments() {
		if (mirroredObject.loadDocumentsRouted()) {
			return documentCollection.findAll(spaceObjectFilter);
		}
		return documentCollection.findAll();
	}

	private Optional<LoadedDocument<T>> tryPatchAndConvert(DBObject object) {
		try {
			Optional<LoadedDocument<T>> result = patchAndConvert(new BasicDBObject(object.toMap()));
			long loaded = numLoadedObjects.incrementAndGet();
			if (loaded % 10000 == 0) {
				log.info("Status: loaded {} records for collection {}", loaded, mirroredObject.getCollectionName());
			}
			return result;
		} catch (RuntimeException e) {
			LOGGER.error("Unable to load dbObject=" + object, e);
			throw e;
		}
	}

	Optional<LoadedDocument<T>> loadById(Object id) {
		BasicDBObject document = findById(id);
		if (document == null) {
			return Optional.empty();
		}
		// TODO: Why throw when spaceObjectFilter rejects but not when not found by findById???
		LoadedDocument<T> result = patchAndConvert(document).orElseThrow(() -> new IllegalArgumentException("Space object not accepted by filter (id=" + id + ")"));
		return Optional.of(result);
	}
	
	List<LoadedDocument<T>> loadByQuery(T template) {
		return documentCollection.findByQuery(documentConverter.toQuery(template))
								.map(BasicDBObject.class::cast)
								.map(this::patchAndConvert)
								.flatMap(OptionalUtil::asStream)
								.collect(Collectors.toList());
	}

	private BasicDBObject findById(Object id) {
		final Object convertedId = documentConverter.convertToMongoObject(id);
		final DBObject dbObject = documentCollection.findById(convertedId);
		return dbObject != null ? new BasicDBObject(dbObject.toMap()) : null;
	}

	private Optional<LoadedDocument<T>> patchAndConvert(BasicDBObject dbObject) {
		BasicDBObject currentVersion = dbObject;
		boolean patched = false;
		if (this.mirroredObject.requiresPatching(dbObject)) {
			patched = true;
			try {
				currentVersion = this.mirroredObject.patch(dbObject);
			} catch (RuntimeException e) {
				LOGGER.error("Patch of document failed! document=" + mirroredObject + "dbObject=" + dbObject, e);
				throw e;
			}
		}
		T mirroredObject = documentConverter.convert(this.mirroredObject.getMirroredType(), currentVersion);
		if (!spaceObjectFilter.accept(mirroredObject)) {
			return Optional.empty();
		}
		if (patched) {
			return Optional.of(new LoadedDocument<T>(postProcess(mirroredObject), Optional.of(new PatchedDocument(dbObject, currentVersion))));
		}
		return Optional.of(new LoadedDocument<T>(postProcess(mirroredObject), Optional.empty()));
	}

	private T postProcess(T mirroredObject) {
		if (mirroredObject instanceof ReloadableSpaceObject) {
			ReloadableSpaceObjectUtil.markReloaded((ReloadableSpaceObject) mirroredObject);
		}
		return mirroredObject;
	}

	/*
	 * Holds the space representation of a document loaded form an external data source (typically mongo)
	 * and also an Optional {@link PatchedDocument} which is present if the document was patched during
	 * the loading 
	 */
	static class LoadedDocument<T> {
		private final T document;
		private final Optional<PatchedDocument> patchedDocument;

		public LoadedDocument(T document, Optional<PatchedDocument> patchedDocument) {
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