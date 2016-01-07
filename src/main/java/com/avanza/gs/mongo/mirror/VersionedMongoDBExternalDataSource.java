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

import org.openspaces.core.cluster.ClusterInfo;
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
import com.gigaspaces.sync.OperationsBatchData;
/**
 * @author Elias Lindholm (elilin)
 *
 */
final class VersionedMongoDBExternalDataSource {

	private static final Logger logger = LoggerFactory.getLogger(VersionedMongoDBExternalDataSource.class);

	private final SpaceMirrorContext spaceMirror;
	private final MirroredDocumentWriter mirroredDocumentWriter;
	private ClusterInfo clusterInfo;
	private volatile String mbeanName = null;
	private final ToggleableDocumentWriteExceptionHandler exceptionHandler;
	private final MBeanRegistrator mbeanRegistrator;

	public VersionedMongoDBExternalDataSource(SpaceMirrorContext spaceMirror) {
		this.spaceMirror = spaceMirror;
		exceptionHandler = ToggleableDocumentWriteExceptionHandler.create(
				new RethrowsTransientDocumentWriteExceptionHandler(),
				new CatchesAllDocumentWriteExceptionHandler());
		this.mirroredDocumentWriter = new MirroredDocumentWriter(spaceMirror, exceptionHandler);
		this.mbeanRegistrator = new PlatformMBeanServerMBeanRegistrator();
	}

	public void executeBulk(OperationsBatchData batch) {
		mirroredDocumentWriter.executeBulk(batch);
	}


	@SuppressWarnings("unchecked")
	DataIterator<Object> initialLoad() {
		// TODO: Reimplement inital-load to avoid loading and holding all spaceobjects in memory before writing them to space.
		List<Iterator<Object>> mongoData = new LinkedList<>();
		for (MirroredDocument<?> mirroredDocument : spaceMirror.getMirroredDocuments()) {
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

	<T extends ReloadableSpaceObject> T reloadObject(Class<T> spaceType, Object documentId) {
		MirroredDocument<T> mirroredDocument = spaceMirror.getMirroredDocument(spaceType);
		MirroredDocumentLoader<T> documentLoader = spaceMirror.createDocumentLoader(mirroredDocument, getPartitionId(), getPartitionCount());
		documentLoader.loadById(documentId);
		List<T> result = writeBack(mirroredDocument, documentLoader);
		return result.isEmpty() ? null : result.get(0);
	}

	<T> Collection<T> loadObjects(Class<T> spaceType, T template) {
		MirroredDocument<T> mirroredDocument = spaceMirror.getMirroredDocument(spaceType);
		MirroredDocumentLoader<T> documentLoader = spaceMirror.createDocumentLoader(mirroredDocument, getPartitionId(), getPartitionCount());
		documentLoader.loadByQuery(template);
		return writeBack(mirroredDocument, documentLoader);
	}

	protected <T> Iterable<T> loadInitialLoadData(MirroredDocument<T> document) {
		logger.info("Loading all documents for type: {}", document.getMirroredType().getName());
		MirroredDocumentLoader<T> documentLoader = spaceMirror.createDocumentLoader(document, getPartitionId(), getPartitionCount());
		if (document.writeBackPatchedDocuments()) {
			documentLoader.loadAllObjects();
			return writeBack(document, documentLoader);
		} else {
			return documentLoader.createPatchingIterable();
		}
	}

	private <T> List<T> writeBack(MirroredDocument<T> document, MirroredDocumentLoader<T> documentLoader) {
		writePatchedDocumentsToStore(spaceMirror.getDocumentCollection(document), document, documentLoader.getPendingPatchedDocuments());
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

	private Integer getPartitionCount() {
		return clusterInfo.getNumberOfInstances();
	}

	private Integer getPartitionId() {
		return clusterInfo.getInstanceId();
	}

	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

	void registerExceptionHandlerMBean() {
		MBeanRegistrationUtil.registerExceptionHandlerMBean(mbeanRegistrator, exceptionHandler);
	}
	
	public void deregisterExceptionHandlerMBean() {
		mbeanRegistrator.unregisterMBean(mbeanName);
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
