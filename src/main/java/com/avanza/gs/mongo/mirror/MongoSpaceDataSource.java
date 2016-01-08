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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.PreDestroy;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.gs.mongo.CatchesAllDocumentWriteExceptionHandler;
import com.avanza.gs.mongo.ParallelIteratorIterator;
import com.avanza.gs.mongo.RethrowsTransientDocumentWriteExceptionHandler;
import com.avanza.gs.mongo.ToggleableDocumentWriteExceptionHandler;
import com.avanza.gs.mongo.mbean.MBeanRegistrationUtil;
import com.avanza.gs.mongo.mbean.MBeanRegistrator;
import com.avanza.gs.mongo.mbean.PlatformMBeanServerMBeanRegistrator;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.SpaceDataSource;

final class MongoSpaceDataSource extends SpaceDataSource implements ClusterInfoAware, SpaceObjectLoader {

	private static final Logger logger = LoggerFactory.getLogger(MongoSpaceDataSource.class);
	
	private final SpaceMirrorContext spaceMirrorContext;
	private ClusterInfo clusterInfo;
	private final ToggleableDocumentWriteExceptionHandler exceptionHandler;
	private final MBeanRegistrator mbeanRegistrator;

	public MongoSpaceDataSource(SpaceMirrorContext spaceMirror) {
		this.spaceMirrorContext = spaceMirror;
		exceptionHandler = ToggleableDocumentWriteExceptionHandler.create(
				new RethrowsTransientDocumentWriteExceptionHandler(),
				new CatchesAllDocumentWriteExceptionHandler());
		this.mbeanRegistrator = new PlatformMBeanServerMBeanRegistrator();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public DataIterator<Object> initialDataLoad() {
		// TODO: Reimplement inital-load to avoid loading and holding all spaceobjects in memory before writing them to space.
		List<Iterator<Object>> mongoData = new LinkedList<>();
		for (MirroredDocument<?> mirroredDocument : spaceMirrorContext.getMirroredDocuments()) {
			if (mirroredDocument.excludeFromInitialLoad()) {
				continue;
			}
			// We always use the "preferred collection name" here. If we use Spring data's default
			// collection name lookup it triggers application lifecycle events which often
			// messes up the entire Spring context loading by trying to load beans that are not
			// initialized fully yet.
			mongoData.add((Iterator<Object>)loadInitialLoadData(mirroredDocument).iterator());
		}
		return new IteratorAdapter(ParallelIteratorIterator.create(mongoData));
	}
	
	<T> Iterable<T> loadInitialLoadData(MirroredDocument<T> document) {
		logger.info("Loading all documents for type: {}", document.getMirroredType().getName());
		MirroredDocumentLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(document, getPartitionId(), getPartitionCount());
		if (document.writeBackPatchedDocuments()) {
			documentLoader.loadAllObjects();
			return writeBack(document, documentLoader);
		} else {
			return documentLoader.createPatchingIterable();
		}
	}
	
	private <T> List<T> writeBack(MirroredDocument<T> document, MirroredDocumentLoader<T> documentLoader) {
		writePatchedDocumentsToStore(spaceMirrorContext.getDocumentCollection(document), document, documentLoader.getPendingPatchedDocuments());
		List<T> loadedSpaceObjects = documentLoader.getLoadedSpaceObjects();
		logger.info("Done loading documents for " + document.getMirroredType().getName() + ". Read object count: " + loadedSpaceObjects.size());
		return loadedSpaceObjects;
	}

	private <T> void writePatchedDocumentsToStore(DocumentCollection documentStore, MirroredDocument<T> document, List<PatchedDocument> patchedDocuments) {
		for (PatchedDocument patchedDocument : patchedDocuments) {
			documentStore.replace(patchedDocument.getOldVersion(), patchedDocument.getNewVersion());
		}
		logger.debug("Updated {} documents in db for {}", patchedDocuments.size(), document.getMirroredType().getName());
	}

	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

	@Override
	public <T extends ReloadableSpaceObject> T reloadObject(Class<T> spaceType, Object documentId) {
		MirroredDocument<T> mirroredDocument = spaceMirrorContext.getMirroredDocument(spaceType);
		MirroredDocumentLoader<T> documentLoader = spaceMirrorContext.createDocumentLoader(mirroredDocument, getPartitionId(), getPartitionCount());
		documentLoader.loadById(documentId);
		List<T> result = writeBack(mirroredDocument, documentLoader);
		return result.isEmpty() ? null : result.get(0);
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
		documentLoader.loadByQuery(template);
		return writeBack(mirroredDocument, documentLoader);
	}
	
	@PreDestroy
	public void destroy() {
		MBeanRegistrationUtil.registerExceptionHandlerMBean(mbeanRegistrator, exceptionHandler);
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
