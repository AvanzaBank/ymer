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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.ymer.MirroredDocumentLoader.LoadedDocument;
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
		// TODO: Reimplement inital-load to avoid loading and holding all spaceobjects in memory before writing them to space.
		Iterator<Object> mongoData = 
				spaceMirrorContext.getMirroredDocuments().stream()
														 .filter(md -> !md.excludeFromInitialLoad())
														 .flatMap(md -> load((MirroredDocument<Object>) md))
														 .iterator();
		return new IteratorAdapter(mongoData);
	}
	
	/*
	 * Returns all documents for a given space object type. The documents will be patched during the load and upgraded
	 * to the most recent document format before transforming it to an space object. If the document was changed (patched)
	 * during the load it will also be written back to the document source (i.e mongo database).
	 */
	<T> Stream<T> load(MirroredDocument<T> document) {
		logger.info("Loading all documents for type: {}", document.getMirroredType().getName());
		MirroredDocumentLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(document, getPartitionId(), getPartitionCount());
		List<LoadedDocument<T>> loadedDocuments = documentLoader.loadAllObjects();
		
		writeBackPatchedDocuments(document, loadedDocuments);
		return loadedDocuments.stream()
							  .map(LoadedDocument::getDocument);
	}
	
	private <T> void writeBackPatchedDocuments(MirroredDocument<T> document, List<LoadedDocument<T>> loadedDocuments) {
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
		MirroredDocument<T> mirroredDocument = spaceMirrorContext.getMirroredDocument(spaceType);
		MirroredDocumentLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(mirroredDocument, getPartitionId(), getPartitionCount());
		Optional<LoadedDocument<T>> loadDocument = documentLoader.loadById(documentId);
		writeBackPatchedDocuments(mirroredDocument, loadDocument.map(Arrays::asList).orElse(Collections.emptyList()));
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
		MirroredDocument<T> mirroredDocument = spaceMirrorContext.getMirroredDocument(spaceType);
		MirroredDocumentLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(mirroredDocument, getPartitionId(), getPartitionCount());
		List<LoadedDocument<T>> loadedDocuments = documentLoader.loadByQuery(template);
		writeBackPatchedDocuments(mirroredDocument, loadedDocuments);
		return loadedDocuments
					  .stream()
					  .map(LoadedDocument::getDocument)
					  .collect(Collectors.toList());
	}
	
	private static class IteratorAdapter implements DataIterator<Object> {

		private final Iterator<Object> it;

		public IteratorAdapter(Iterator<Object> it) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
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

	
}
