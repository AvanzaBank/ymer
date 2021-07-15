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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.avanza.ymer.plugin.PreWriteProcessor;

/**
 * Holds the runtime context for a mongo mirror. <p>
 *
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class SpaceMirrorContext {

	public static final MirrorExceptionListener NO_EXCEPTION_LISTENER = (e, failedOperation, failedObjects) -> {};

	private final MirroredObjects mirroredObjects;
	private final DocumentConverter documentConverter;
	private final Map<Class<?>, DocumentCollection> documentCollectionByMirroredType = new ConcurrentHashMap<>();
	private final DocumentDb documentDb;
	private final MirrorExceptionListener mirrorExceptionListener;
	private final Plugins plugins;
	private final int numParallelCollections;

	SpaceMirrorContext(MirroredObjects mirroredObjects, DocumentConverter documentConverter, DocumentDb documentDb, MirrorExceptionListener mirrorExceptionListener, Plugins plugins, int numParallelCollections) {
		this.documentDb = Objects.requireNonNull(documentDb);
		this.mirrorExceptionListener = Objects.requireNonNull(mirrorExceptionListener);
		this.mirroredObjects = Objects.requireNonNull(mirroredObjects);
		this.documentConverter = Objects.requireNonNull(documentConverter);
		this.plugins = Objects.requireNonNull(plugins);
		this.numParallelCollections = numParallelCollections;

		for (MirroredObject<?> mirroredObject : mirroredObjects.getMirroredObjects()) {
			DocumentCollection documentCollection = documentDb.getCollection(
					mirroredObject.getCollectionName(),
					mirroredObject.getReadPreference()
			);
			this.documentCollectionByMirroredType.put(mirroredObject.getMirroredType(), documentCollection);
		}
	}

	boolean isMirroredType(Class<?> type) {
		return this.mirroredObjects.isMirroredType(type);
	}

	String getCollectionName(Class<?> type) {
		return mirroredObjects.getMirroredObject(type).getCollectionName();
	}

	DocumentCollection getDocumentCollection(Class<?> type) {
		return documentCollectionByMirroredType.get(type);
	}

	DocumentCollection getDocumentCollection(MirroredObject<?> document) {
		return getDocumentCollection(document.getMirroredType());
	}

	<T> MirroredObjectLoader<T> createDocumentLoader(MirroredObject<T> document, int partitionId, int partitionCount) {
		DocumentCollection documentCollection = getDocumentCollection(document.getMirroredType());
		return new MirroredObjectLoader<>(
				documentCollection,
				documentConverter,
				document,
				SpaceObjectFilter.partitionFilter(document, partitionId, partitionCount),
				new MirrorContextProperties(partitionCount, partitionId),
				plugins.getPostReadProcessing(document.getMirroredType()));
	}

	Collection<MirroredObject<?>> getMirroredDocuments() {
		return this.mirroredObjects.getMirroredObjects();
	}

	DocumentConverter getDocumentConverter() {
		return this.documentConverter;
	}

	DocumentDb getDocumentDb() {
		return documentDb;
	}

	public int getNumParallelCollections() {
		return numParallelCollections;
	}

	/**
	 * Converts the given space object to a mongo document and appends
	 * the current document version to the created mongo document. <p>
	 */
	<T> Document toVersionedDocument(T spaceObject, int partitionCount) {
		@SuppressWarnings("unchecked")
		MirroredObject<T> mirroredObject = (MirroredObject<T>) this.mirroredObjects.getMirroredObject(spaceObject.getClass());
		Document document = this.documentConverter.convertToBsonDocument(spaceObject);
		mirroredObject.setDocumentAttributes(document, spaceObject, partitionCount);
		return document;
	}

	<T> MirroredObject<T> getMirroredDocument(Class<T> type) {
		return this.mirroredObjects.getMirroredObject(type);
	}

	void onMirrorException(Exception e, MirrorOperation operation, Object... faieldObjects) {
		this.mirrorExceptionListener.onMirrorException(e, operation, faieldObjects);
	}

	public boolean keepPersistent(Class<?> type) {
		return this.mirroredObjects.isMirroredType(type)
				&& getMirroredDocument(type).keepPersistent();
	}

	public PreWriteProcessor getPreWriteProcessing(Class<?> spaceClass) {
		return plugins.getPreWriteProcessing(spaceClass);
	}
}
