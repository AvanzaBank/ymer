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

import static com.avanza.ymer.MirroredObject.DOCUMENT_INSTANCE_ID;
import static com.avanza.ymer.MirroredObject.DOCUMENT_ROUTING_KEY;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.IndexOptions;

public class PersistedInstanceIdRecalculationService implements PersistedInstanceIdRecalculationServiceMBean {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final SpaceMirrorContext spaceMirror;

	public PersistedInstanceIdRecalculationService(SpaceMirrorContext spaceMirror) {
		this.spaceMirror = spaceMirror;
	}

	@Override
	public void recalculatePersistedInstanceId(String collectionName, int numberOfInstances) {
		log.info("Recalculating persisted instance id for collection {} with {} number of instances", collectionName, numberOfInstances);
		String indexSuffix = "_" + numberOfInstances;
		DocumentCollection collection = spaceMirror.getDocumentDb().getCollection(collectionName);
		boolean indexDropped = collection.getIndexes()
				.filter(index -> index.isIndexForFields(Set.of(DOCUMENT_INSTANCE_ID)))
				.filter(index -> !index.getName().endsWith(indexSuffix))
				.peek(index -> {
					log.info("Step 1/3\tDropping index {}", index.getName());
					collection.dropIndex(index.getName());
				})
				.findAny()
				.isPresent();
		if (!indexDropped) {
			log.info("Step 1/3\tNo index to drop");
		}
		AtomicInteger count = new AtomicInteger();
		collection.findByQuery(query(where(DOCUMENT_ROUTING_KEY).exists(true)))
				.peek(it -> {
					int currentCount = count.incrementAndGet();
					if (currentCount % 10_000 == 0) {
						log.info("Step 2/3\tUpdated persisted instance id for {} documents", currentCount);
					}
				})
				.forEach(document -> {
					Object id = document.get("_id");
					int instanceId = getInstanceId(document.get(DOCUMENT_ROUTING_KEY), numberOfInstances);
					collection.updateById(id, Map.of(DOCUMENT_INSTANCE_ID, instanceId));
				});

		log.info("Step 2/3\tUpdated persisted instance id for {} documents", count.get());

		boolean indexExists = collection.getIndexes()
				.filter(index -> index.isIndexForFields(Set.of(DOCUMENT_INSTANCE_ID)))
				.anyMatch(index -> index.getName().endsWith(indexSuffix));
		if (indexExists) {
			log.info("Step 3/3\tIndex does not need to be created because it already exists");
		} else {
			String indexName = "_index" + DOCUMENT_INSTANCE_ID + "_numberOfInstances" + indexSuffix;
			log.info("Step 3/3\tCreating index {}", indexName);
			IndexOptions options = new IndexOptions().name(indexName).background(true);
			collection.createIndex(new Document(DOCUMENT_INSTANCE_ID, 1), options);
			log.info("Step 3/3\tDone creating index");
		}
	}

}
