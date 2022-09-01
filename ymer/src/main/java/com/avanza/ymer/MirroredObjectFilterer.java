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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

class MirroredObjectFilterer {

	private static final Logger logger = LoggerFactory.getLogger(MirroredObjectFilterer.class);

	private final SpaceMirrorContext mirror;

	public MirroredObjectFilterer(SpaceMirrorContext mirror) {
		this.mirror = mirror;
	}

	public Collection<DataSyncOperation> filterSpaceObjects(DataSyncOperation[] batchDataItems) {
		List<DataSyncOperation> result = new ArrayList<>(batchDataItems.length);
		for (DataSyncOperation bulkItem : batchDataItems) {
			if (isReloaded(bulkItem)) {
				continue;
			}
			if (bulkItem.getDataSyncOperationType() == DataSyncOperationType.REMOVE
					&& mirror.keepPersistent(bulkItem.getDataAsObject().getClass())) {
				continue;
			}
			if (!mirror.isMirroredType(bulkItem.getDataAsObject().getClass())) {
				logger.debug("Ignored {}, not a mirrored class", bulkItem.getDataAsObject().getClass().getName());
				continue;
			}
			result.add(bulkItem);
		}
		return result;
	}

	private boolean isReloaded(DataSyncOperation bulkItem) {
		Object item = bulkItem.getDataAsObject();
		return (bulkItem.getDataSyncOperationType() == DataSyncOperationType.WRITE || bulkItem.getDataSyncOperationType() == DataSyncOperationType.UPDATE)
				&& item instanceof ReloadableSpaceObject
				&& ReloadableSpaceObjectUtil.isReloaded((ReloadableSpaceObject) item);
	}

}
