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

import com.mongodb.BasicDBObject;
/**
 * Holds the runtime context for a mongo mirror. <p>
 *
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class SpaceMirrorContext {

	private static MirrorExceptionListener NO_EXCEPTION_LISTENER = new MirrorExceptionListener() {
		@Override
		public void onMirrorException(Exception e, MirrorOperation failedOperation, Object[] failedObjects) {}
	};

	private final MirroredDocuments mirroredDocuments;
	private final DocumentConverter documentConverter;
	private final Map<Class<?>, DocumentCollection> documentCollectionByMirroredType = new ConcurrentHashMap<>();
	private final DocumentDb documentDb;
	private final MirrorExceptionListener mirrorExceptionListener;

	SpaceMirrorContext(MirroredDocuments mirroredDocuments, DocumentConverter documentConverter, DocumentDb documentDb) {
		this(mirroredDocuments, documentConverter, documentDb, NO_EXCEPTION_LISTENER);
	}

	SpaceMirrorContext(MirroredDocuments mirroredDocuments, DocumentConverter documentConverter, DocumentDb documentDb, MirrorExceptionListener mirrorExceptionListener) {
		this.documentDb = Objects.requireNonNull(documentDb);
		this.mirrorExceptionListener = Objects.requireNonNull(mirrorExceptionListener);
		this.mirroredDocuments = Objects.requireNonNull(mirroredDocuments);
		this.documentConverter = Objects.requireNonNull(documentConverter);
		for (MirroredDocument<?> mirroredDocument : mirroredDocuments.getMirroredDocuments()) {
			DocumentCollection documentCollection = documentDb.getCollection(mirroredDocument.getCollectionName());
			this.documentCollectionByMirroredType.put(mirroredDocument.getMirroredType(), documentCollection);
		}
	}

	boolean isMirroredType(Class<?> type) {
		return this.mirroredDocuments.isMirroredType(type);
	}

	String getCollectionName(Class<?> type) {
		return mirroredDocuments.getMirroredDocument(type).getCollectionName();
	}

	DocumentCollection getDocumentCollection(Class<?> type) {
		return documentCollectionByMirroredType.get(type);
	}

	DocumentCollection getDocumentCollection(MirroredDocument<?> document) {
		return getDocumentCollection(document.getMirroredType());
	}

	<T> MirroredDocumentLoader<T> createDocumentLoader(MirroredDocument<T> document, int partitionId, int partitionCount) {
		DocumentCollection documentCollection = getDocumentCollection(document.getMirroredType());
		return new MirroredDocumentLoader<>(documentCollection, documentConverter, document, SpaceObjectFilter.partitionFilter(document, partitionId, partitionCount));
	}

	Collection<MirroredDocument<?>> getMirroredDocuments() {
		return this.mirroredDocuments.getMirroredDocuments();
	}

	DocumentConverter getDocumentConverter() {
		return this.documentConverter;
	}

	DocumentDb getDocumentDb() {
		return documentDb;
	}

	/**
	 * Converts the given space object to a mongo document and appends
	 * the current document version to the created mongo document. <p>
	 */
	<T> BasicDBObject toVersionedDbObject(T spaceObject) {
		@SuppressWarnings("unchecked")
		MirroredDocument<T> mirroredDocument = (MirroredDocument<T>) this.mirroredDocuments.getMirroredDocument(spaceObject.getClass());
		BasicDBObject dbObject = this.documentConverter.convertToDBObject(spaceObject);
		mirroredDocument.setDocumentAttributes(dbObject, spaceObject);
		return dbObject;
	}

	<T> MirroredDocument<T> getMirroredDocument(Class<T> type) {
		return this.mirroredDocuments.getMirroredDocument(type);
	}

	void onMirrorException(Exception e, MirrorOperation operation, Object... faieldObjects) {
		this.mirrorExceptionListener.onMirrorException(e, operation, faieldObjects);
	}

	public boolean keepPersistent(Class<?> type) {
		return this.mirroredDocuments.isMirroredType(type)
				&& getMirroredDocument(type).keepPersistent();
	}
}
