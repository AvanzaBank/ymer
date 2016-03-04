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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.ymer.MirroredObjectLoader.LoadedDocument;
import com.avanza.ymer.util.OptionalUtil;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.SpaceDataSource;

final class YmerSpaceDataSource extends SpaceDataSource implements ClusterInfoAware, SpaceObjectLoader {

	private static final Logger logger = LoggerFactory.getLogger(YmerSpaceDataSource.class);
	
	private final SpaceMirrorContext spaceMirrorContext;
	private ClusterInfo clusterInfo;

	public YmerSpaceDataSource(SpaceMirrorContext spaceMirror) {
		this.spaceMirrorContext = spaceMirror;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public DataIterator<Object> initialDataLoad() {
		InitialLoadCompleteDispatcher initialLoadCompleteDispatcher = new InitialLoadCompleteDispatcher();
		Iterator<Object> mongoData = 
				spaceMirrorContext.getMirroredDocuments().stream()
														 .filter(md -> !md.excludeFromInitialLoad())
														 .flatMap(md -> load((MirroredObject<Object>) md, initialLoadCompleteDispatcher))
														 .iterator();
		return new IteratorAdapter(mongoData, initialLoadCompleteDispatcher::initialLoadComplete);
	}
	
	/*
	 * Returns all documents for a given space object type. The documents will be patched during the load and upgraded
	 * to the most recent document format before transforming it to an space object. If the document was changed (patched)
	 * during the load it will also be written back to the document source (i.e mongo database).
	 */
	<T> Stream<T> load(MirroredObject<T> document, InitialLoadCompleteDispatcher initialLoadCompleteDispatcher) {
		logger.info("Loading all documents for type: {}", document.getMirroredType().getName());
		MirroredObjectLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(document, getPartitionId(), getPartitionCount());
		initialLoadCompleteDispatcher.onInitialLoadComplete(documentLoader::destroy);
		Stream<LoadedDocument<T>> loadedDocumentStream = documentLoader.streamAllObjects();
		return loadedDocumentStream.map(writeBackPatchedDocument(document, initialLoadCompleteDispatcher));
	}
	
	private <T> Function<LoadedDocument<T>, T> writeBackPatchedDocument(MirroredObject<T> document, InitialLoadCompleteDispatcher initialLoadCompleteDispatcher) {
		AtomicInteger totalWritebackCount = new AtomicInteger(0);
		initialLoadCompleteDispatcher.onInitialLoadComplete(() -> logger.debug("Updated {} documents in db for {}", totalWritebackCount.get(), document.getMirroredType().getName()));
		return loadedDocument -> {
			if (document.writeBackPatchedDocuments()) {
				patchDocument(document, loadedDocument);
				totalWritebackCount.incrementAndGet();
			}
			return loadedDocument.getDocument();
		};
	}

	private <T> void patchDocument(MirroredObject<T> document, LoadedDocument<T> loadedDocument) {
		loadedDocument.getPatchedDocument().ifPresent(patchedDocument -> {
					DocumentCollection documentCollection = spaceMirrorContext.getDocumentCollection(document);
					documentCollection.replace(patchedDocument.getOldVersion(), patchedDocument.getNewVersion());
				});
	}

	private <T> void writeBackPatchedDocuments(MirroredObject<T> document, List<LoadedDocument<T>> loadedDocuments) {
		if (!document.writeBackPatchedDocuments()) {
			return;
		}
		long patchCount = loadedDocuments.stream()
					   .map(LoadedDocument::getPatchedDocument)
					   .flatMap(OptionalUtil::asStream)
					   .map(patchedDocument -> {
						   DocumentCollection documentCollection = spaceMirrorContext.getDocumentCollection(document);
						   documentCollection.replace(patchedDocument.getOldVersion(), patchedDocument.getNewVersion());
						   return patchedDocument;
					   }).count();
		logger.debug("Updated {} documents in db for {}", patchCount, document.getMirroredType().getName());
	}
	
	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

	@Override
	public <T extends ReloadableSpaceObject> T reloadObject(Class<T> spaceType, Object documentId) {
		MirroredObject<T> mirroredObject = spaceMirrorContext.getMirroredDocument(spaceType);
		MirroredObjectLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(mirroredObject, getPartitionId(), getPartitionCount());
		Optional<LoadedDocument<T>> loadDocument = documentLoader.loadById(documentId);
		writeBackPatchedDocuments(mirroredObject, loadDocument.map(Arrays::asList).orElse(Collections.emptyList()));
		return loadDocument
					   .map(LoadedDocument::getDocument)
					   .orElse(null);
	}

	private Integer getPartitionCount() {
		return clusterInfo.getNumberOfInstances();
	}

	private Integer getPartitionId() {
		return clusterInfo.getInstanceId();
	}
	
	@Override
	public <T> Collection<T> loadObjects(Class<T> spaceType, T template) {
		MirroredObject<T> mirroredObject = spaceMirrorContext.getMirroredDocument(spaceType);
		MirroredObjectLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(mirroredObject, getPartitionId(), getPartitionCount());
		List<LoadedDocument<T>> loadedDocuments = documentLoader.loadByQuery(template);
		writeBackPatchedDocuments(mirroredObject, loadedDocuments);
		return loadedDocuments
					  .stream()
					  .map(LoadedDocument::getDocument)
					  .collect(Collectors.toList());
	}
	
	private static class IteratorAdapter implements DataIterator<Object> {

		private final Iterator<Object> it;
		private final Runnable iterationDone;

		public IteratorAdapter(Iterator<Object> it, Runnable itrationDoneCallback) {
			this.it = it;
			this.iterationDone = itrationDoneCallback;
		}

		@Override
		public boolean hasNext() {
			boolean hasNext = it.hasNext();
			if (!hasNext) {
				iterationDone.run();
			}
			return hasNext;
		}

		@Override
		public Object next() {
			return it.next();
		}

		@Override
		public void remove() {
			it.remove();
		}

		@Override
		public void close() {
		}

	}
	
	static class InitialLoadCompleteDispatcher {
		private final List<Runnable> l = new CopyOnWriteArrayList<>();
		public void onInitialLoadComplete(Runnable callback) {
			l.add(callback);
		}
		public void initialLoadComplete() {
			l.stream().forEach(Runnable::run);
		}
	}
}
