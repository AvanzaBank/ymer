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
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.util.StreamUtils;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.IJSpace;
import com.mongodb.client.model.IndexOptions;

public class PersistedInstanceIdRecalculationService implements PersistedInstanceIdRecalculationServiceMBean, ApplicationContextAware {
	private static final String NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY = "cluster.partitions";
	private static final int BATCH_SIZE = 10_000;

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final SpaceMirrorContext spaceMirror;

	@Nullable
	private ApplicationContext applicationContext;

	public PersistedInstanceIdRecalculationService(SpaceMirrorContext spaceMirror) {
		this.spaceMirror = spaceMirror;
	}

	@Override
	public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public boolean collectionNeedsCalculation(String collectionName) {
		try {
			int numberOfPartitions = determineNumberOfPartitions();
			DocumentCollection collection = spaceMirror.getDocumentDb().getCollection(collectionName);
			return collection.getIndexes()
					.filter(index -> index.isIndexForFields(Set.of(DOCUMENT_INSTANCE_ID)))
					.noneMatch(isIndexForNumberOfPartitions(numberOfPartitions));
		} catch (Exception e) {
			log.warn("Could not determine whether persisted instance id should be recalculated for collection " + collectionName, e);
			return false;
		}
	}

	@Override
	public void recalculatePersistedInstanceId() {
		int numberOfPartitions = determineNumberOfPartitions();

		spaceMirror.getMirroredDocuments().stream()
				.filter(MirroredObject::persistInstanceId)
				.map(MirroredObject::getCollectionName)
				.forEach(collectionName -> recalculatePersistedInstanceId(collectionName, numberOfPartitions));
	}

	@Override
	public void recalculatePersistedInstanceId(String collectionName) {
		int numberOfPartitions = determineNumberOfPartitions();
		recalculatePersistedInstanceId(collectionName, numberOfPartitions);
	}

	private void recalculatePersistedInstanceId(String collectionName, int numberOfPartitions) {
		log.info("Recalculating persisted instance id for collection {} with {} number of partitions", collectionName, numberOfPartitions);
		DocumentCollection collection = spaceMirror.getDocumentDb().getCollection(collectionName);
		boolean indexDropped = collection.getIndexes()
				.filter(index -> index.isIndexForFields(Set.of(DOCUMENT_INSTANCE_ID)))
				.filter(not(isIndexForNumberOfPartitions(numberOfPartitions)))
				.peek(index -> {
					log.info("Step 1/3\tDropping index {}", index.getName());
					collection.dropIndex(index.getName());
				})
				.findAny()
				.isPresent();
		if (!indexDropped) {
			log.info("Step 1/3\tNo index to drop");
		}

		log.info("Using batch size {}", BATCH_SIZE);
		try (Stream<List<Document>> batches = StreamUtils.buffer(collection.findByQuery(createQuery(BATCH_SIZE)), BATCH_SIZE)) {
			LongAdder analyzedCount = new LongAdder();
			AtomicInteger updatedCount = new AtomicInteger();
			batches.forEach(batch -> collection.bulkWrite(bulkWriter -> {
				Map<Integer, List<Document>> updatesByInstanceId = batch.stream()
						.collect(groupingBy(it -> getInstanceId(it.get(DOCUMENT_ROUTING_KEY), numberOfPartitions)));

				updatesByInstanceId.forEach((instanceId, documents) -> {
					Set<Object> ids = documents.stream()
							.peek(it -> analyzedCount.increment())
							.filter(document -> !Objects.equals(instanceId, document.get(DOCUMENT_INSTANCE_ID)))
							.map(document -> document.get("_id"))
							.filter(Objects::nonNull)
							.peek(it -> {
								int currentCount = updatedCount.incrementAndGet();
								if (currentCount % 10_000 == 0) {
									log.info("Step 2/3\tUpdated persisted instance id for {} documents ({} analyzed)", currentCount, analyzedCount.sum());
								}
							})
							.collect(toSet());
					if (!ids.isEmpty()) {
						bulkWriter.updatePartialByIds(ids, Map.of(DOCUMENT_INSTANCE_ID, instanceId));
					}
				});
			}));
			log.info("Step 2/3\tUpdated persisted instance id for {} documents total ({} analyzed total)", updatedCount.get(), analyzedCount.sum());
		}

		boolean indexExists = collection.getIndexes()
				.filter(index -> index.isIndexForFields(Set.of(DOCUMENT_INSTANCE_ID)))
				.anyMatch(isIndexForNumberOfPartitions(numberOfPartitions));
		if (indexExists) {
			log.info("Step 3/3\tIndex does not need to be created because it already exists");
		} else {
			String indexName = "_index" + DOCUMENT_INSTANCE_ID + "_numberOfPartitions_" + numberOfPartitions;
			log.info("Step 3/3\tCreating index {}", indexName);
			IndexOptions options = new IndexOptions().name(indexName).background(true);
			collection.createIndex(new Document(DOCUMENT_INSTANCE_ID, 1), options);
			log.info("Step 3/3\tDone creating index");
		}
	}

	private Predicate<IndexInfo> isIndexForNumberOfPartitions(int numberOfPartitions) {
		String indexSuffix = "_numberOfPartitions_" + numberOfPartitions;
		return index -> index.getName().endsWith(indexSuffix);
	}

	private int determineNumberOfPartitions() {
		return getNumberOfPartitionsFromSpaceProperties()
				.or(this::getNumberOfPartitionsFromSystemProperty)
				.orElseThrow(() -> new IllegalStateException(
						String.format("Could not determine number of partitions, checked space property \"%s\" and system property \"%s\"",
									  MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY))
				);
	}

	private Optional<Integer> getNumberOfPartitionsFromSpaceProperties() {
		return Optional.ofNullable(applicationContext)
				.map(it -> it.getBeanProvider(IJSpace.class).getIfAvailable())
				.map(IJSpace::getDirectProxy)
				.map(IDirectSpaceProxy::getSpaceImplIfEmbedded)
				.map(SpaceImpl::getConfigReader)
				.map(it -> it.getIntSpaceProperty(MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, "0"))
				.filter(it -> it > 0)
				.map(peek(numberOfPartitions ->
								  log.info("Using {} number of partitions (from space property \"{}\")", numberOfPartitions, MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT)
				));
	}

	private Optional<Integer> getNumberOfPartitionsFromSystemProperty() {
		return Optional.ofNullable(System.getProperty(NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY))
				.map(Integer::valueOf)
				.map(peek(numberOfPartitions ->
								  log.info("Using {} number of partitions (from system property \"{}\")", numberOfPartitions, NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY)
				));
	}

	@SuppressWarnings("SameParameterValue")
	private Query createQuery(int batchSize) {
		Query query = query(where(DOCUMENT_ROUTING_KEY).exists(true));
		query.fields().include(DOCUMENT_ROUTING_KEY).include(DOCUMENT_INSTANCE_ID);
		return query.cursorBatchSize(batchSize);
	}

	private static <T> UnaryOperator<T> peek(Consumer<T> consumer) {
		return it -> {
			consumer.accept(it);
			return it;
		};
	}

}
