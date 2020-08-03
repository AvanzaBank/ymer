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

import com.avanza.ymer.plugin.PostReadProcessor;
import com.avanza.ymer.util.OptionalUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Loads mirrored objects from an external (persistent) source.
 * <p>
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
    private final DocumentConverter documentConverter;
    private final AtomicLong numLoadedObjects = new AtomicLong();
    private final MirrorContextProperties contextProperties;
    private final PostReadProcessor postReadProcessor;

    MirroredObjectLoader(DocumentCollection documentCollection,
                         DocumentConverter documentConverter,
                         MirroredObject<T> mirroredObject,
                         SpaceObjectFilter<T> spaceObjectFilter,
                         MirrorContextProperties contextProperties,
                         PostReadProcessor postReadProcessor) {
        this.documentConverter = documentConverter;
        this.spaceObjectFilter = spaceObjectFilter;
        this.documentCollection = documentCollection;
        this.mirroredObject = mirroredObject;
        this.contextProperties = contextProperties;
        this.postReadProcessor = postReadProcessor;
    }

    List<LoadedDocument<T>> loadAllObjects() {
        return streamAllObjects().collect(Collectors.toList());
    }

    Stream<LoadedDocument<T>> streamAllObjects() {
        log.info("Begin loadAllObjects. targetCollection={}", mirroredObject.getCollectionName());
        return loadDocuments()
                .parallel() // We run patching and conversions in parallel as this is a cpu-intensive task
                .map(this::tryPatchAndConvert)
                .flatMap(OptionalUtil::asStream);
    }

    private Stream<Document> loadDocuments() {
        if (mirroredObject.hasCustomInitialLoadTemplate()) {
            Document template = mirroredObject.getCustomInitialLoadTemplateFactory()
                                                   .create(contextProperties.getPartitionCount(),
                                                           contextProperties.getInstanceId());
            return documentCollection.findByTemplate(template);
        }
        if (mirroredObject.loadDocumentsRouted()) {
            return documentCollection.findAll2(spaceObjectFilter);
        }
        return documentCollection.findAll2();
    }

    private Optional<LoadedDocument<T>> tryPatchAndConvert(Document document) {
        try {
            Optional<LoadedDocument<T>> result;
            try {
                result = patchAndConvert(new Document(document));
            } catch (RuntimeException e) {
                // MongoConverter is not thread-safe due to a bug in AbstractMappingContext.addPersistentEntity().
                // The bug occurs at most once or twice per collection but will produce objects without any properties set
                // Resolve it temporarily by retrying.
                log.warn("Failed to load dbObject=" + document + ". Retrying.", e);
                result = patchAndConvert(new Document(document));
            }
            long loaded = numLoadedObjects.incrementAndGet();
            if (loaded % 10000 == 0) {
                log.info("Status: loaded {} records for collection {}", loaded, mirroredObject.getCollectionName());
            }
            return result;
        } catch (RuntimeException e) {
            log.error("Unable to load document=" + document, e);
            throw e;
        }
    }

    Optional<LoadedDocument<T>> loadById(Object id) {
        Document document = findById(id);
        if (document == null) {
            return Optional.empty();
        }
        // TODO: Why throw when spaceObjectFilter rejects but not when not found by findById???
        LoadedDocument<T> result = patchAndConvert(document).orElseThrow(() -> new IllegalArgumentException("Space object not accepted by filter (id=" + id + ")"));
        return Optional.of(result);
    }

    List<LoadedDocument<T>> loadByQuery(T template) {
        return documentCollection.findByQuery2(documentConverter.toQuery(template))
                .map(this::patchAndConvert)
                .flatMap(OptionalUtil::asStream)
                .collect(Collectors.toList());
    }

    private Document findById(Object id) {
        final Object convertedId = documentConverter.convertToMongoObject(id);
        final Document document = documentCollection.findById2(convertedId);
        return document != null ? new Document(document) : null;
    }

    private Optional<LoadedDocument<T>> patchAndConvert(Document document) {
        Document currentVersion = document;
        boolean patched = false;
        if (this.mirroredObject.requiresPatching(document)) {
            patched = true;
            try {
                currentVersion = new Document(document);
                postReadProcessor.postRead(currentVersion);
                currentVersion = this.mirroredObject.patch(currentVersion);
            } catch (RuntimeException e) {
                log.error("Patch of document failed! document=" + mirroredObject + "currentVersion=" + currentVersion, e);
                throw e;
            }
        } else {
            postReadProcessor.postRead(currentVersion);
        }
        T mirroredObject = documentConverter.convert(this.mirroredObject.getMirroredType(), currentVersion);
        if (!spaceObjectFilter.accept(mirroredObject)) {
            return Optional.empty();
        }
        if (patched) {
            return Optional.of(new LoadedDocument<T>(postProcess(mirroredObject), Optional.of(new PatchedDocumentV2(document, currentVersion))));
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
        private final Optional<PatchedDocumentV2> patchedDocument;

        public LoadedDocument(T document, Optional<PatchedDocumentV2> patchedDocument) {
            this.document = document;
            this.patchedDocument = patchedDocument;
        }

        public T getDocument() {
            return document;
        }

        public Optional<PatchedDocumentV2> getPatchedDocument() {
            return patchedDocument;
        }

    }

}