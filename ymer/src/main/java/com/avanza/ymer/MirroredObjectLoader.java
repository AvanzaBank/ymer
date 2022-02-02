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

import static com.avanza.ymer.PersistedInstanceIdUtil.getInstanceIdFieldName;
import static com.avanza.ymer.PersistedInstanceIdUtil.isIndexForNumberOfPartitions;
import static java.util.stream.Collectors.toList;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.plugin.PostReadProcessor;

/**
 * Loads mirrored objects from an external (persistent) source.
 * <p>
 * Implementation note: multithreaded patching to increase throughput.
 *
 * @author Elias Lindholm (elilin), Kristoffer Erlandsson, Andreas Skoog
 */
final class MirroredObjectLoader<T> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final MirroredObject<T> mirroredObject;
    private final DocumentCollection documentCollection;
    private final SpaceObjectFilter<T> spaceObjectFilter;
    private final DocumentConverter documentConverter;
    private final LongAdder numLoadedObjects = new LongAdder();
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
        return streamAllObjects().collect(toList());
    }

    Stream<LoadedDocument<T>> streamAllObjects() {
        log.info("Begin loadAllObjects. targetCollection={}", mirroredObject.getCollectionName());

        RepeatingTask progressLogger = new RepeatingTask(Duration.ofSeconds(30), () ->
                log.info("Status: loaded {} records for collection {}", numLoadedObjects.sum(), mirroredObject.getCollectionName()));

        return loadDocuments()
                .parallel() // We run patching and conversions in parallel as this is a cpu-intensive task
                .flatMap(document -> tryPatchAndConvert(document).stream())
                .onClose(progressLogger::close);
    }

    private Stream<Document> loadDocuments() {
        if (mirroredObject.hasCustomInitialLoadTemplate()) {
            Document template = mirroredObject.getCustomInitialLoadTemplateFactory()
                                                   .create(contextProperties.getPartitionCount(),
                                                           contextProperties.getInstanceId());
            return documentCollection.findByTemplate(template);
        }
        if (mirroredObject.persistInstanceId()) {
            String instanceIdField = getInstanceIdFieldName(contextProperties.getPartitionCount());
            boolean indexExists = documentCollection.getIndexes()
                    .anyMatch(isIndexForNumberOfPartitions(contextProperties.getPartitionCount()));
            if (indexExists) {
                Query query = query(new Criteria().orOperator(where(instanceIdField).is(contextProperties.getInstanceId()), where(instanceIdField).exists(false)));
                return documentCollection.findByQuery(query);
            } else {
                log.warn("Configured to load using persisted instance id, but no index exists for field {}. Will not use instance id when loading.",
                        instanceIdField);
            }
        }
        if (mirroredObject.loadDocumentsRouted()) {
            return documentCollection.findAll(spaceObjectFilter);
        } else {
            return documentCollection.findAll();
        }
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
                log.warn("Failed to load dbObject={}. Retrying.", document, e);
                result = patchAndConvert(new Document(document));
            }
            numLoadedObjects.increment();

            return result;
        } catch (RuntimeException e) {
            log.error("Unable to load document={}", document, e);
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
        return documentCollection.findByQuery(documentConverter.toQuery(template))
                .flatMap(document -> patchAndConvert(document).stream())
                .collect(toList());
    }

    private Document findById(Object id) {
        final Object convertedId = documentConverter.convertToMongoObject(id);
        final Document document = documentCollection.findById(convertedId);
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
                log.error("Patch of document failed! document={}currentVersion={}", mirroredObject, currentVersion, e);
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
            return Optional.of(new LoadedDocument<>(postProcess(mirroredObject), new PatchedDocument(document, currentVersion)));
        } else {
            return Optional.of(new LoadedDocument<>(postProcess(mirroredObject), null));
        }
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
        private final PatchedDocument patchedDocument;

        public LoadedDocument(T document, @Nullable PatchedDocument patchedDocument) {
            this.document = document;
            this.patchedDocument = patchedDocument;
        }

        public T getDocument() {
            return document;
        }

        public Optional<PatchedDocument> getPatchedDocument() {
            return Optional.ofNullable(patchedDocument);
        }

    }

}
