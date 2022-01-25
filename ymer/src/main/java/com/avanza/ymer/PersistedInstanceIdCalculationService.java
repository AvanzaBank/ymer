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

import static com.avanza.ymer.MirroredObject.DOCUMENT_INSTANCE_ID_PREFIX;
import static com.avanza.ymer.MirroredObject.DOCUMENT_ROUTING_KEY;
import static com.avanza.ymer.PersistedInstanceIdUtil.getInstanceIdFieldName;
import static com.avanza.ymer.PersistedInstanceIdUtil.isIndexForNumberOfPartitions;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.ymer.util.GigaSpacesInstanceIdUtil;
import com.avanza.ymer.util.StreamUtils;
import com.mongodb.client.model.IndexOptions;

public class PersistedInstanceIdCalculationService implements PersistedInstanceIdCalculationServiceMBean, ApplicationContextAware {
	private static final int BATCH_SIZE = 10_000;

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final Map<String, PersistedInstanceIdStatistics> jobStatistics = new ConcurrentHashMap<>();
	private final SpaceMirrorContext spaceMirror;
	private final ReloadableYmerProperties ymerProperties;

	@Nullable
	private ApplicationContext applicationContext;

	public PersistedInstanceIdCalculationService(SpaceMirrorContext spaceMirror, ReloadableYmerProperties ymerProperties) {
		this.spaceMirror = spaceMirror;
		this.ymerProperties = ymerProperties;
	}

	@Override
	public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	PersistedInstanceIdStatisticsMBean collectStatistics(MirroredObject<?> mirroredObject) {
		return getStatisticsForCollection(mirroredObject.getCollectionName());
	}

	// Initializes readyForNumberOfPartitions for collections that are already calculated
	void initializeStatistics() {
		Set<Integer> numberOfPartitionsToCalculate;
		try {
			numberOfPartitionsToCalculate = getNumberOfPartitionsToCalculate();
		} catch (Exception e) {
			log.warn("Could not get number of partitions to calculate, unable to initialize persisted instance id statistics", e);
			return;
		}
		getCollectionsWithPersistInstanceIdEnabled().forEach(collectionName -> {
			try {
				PersistedInstanceIdStatistics statistics = getStatisticsForCollection(collectionName);
				numberOfPartitionsToCalculate.stream()
						.filter(numberOfPartitions -> spaceMirror.getDocumentDb().getCollection(collectionName).getIndexes().anyMatch(isIndexForNumberOfPartitions(numberOfPartitions)))
						.forEach(statistics::addReadyForNumberOfPartitions);
			} catch (Exception e) {
				log.warn("Unable to initialize persisted instance id statistics for collection {}", collectionName, e);
			}
		});
	}

	private PersistedInstanceIdStatistics getStatisticsForCollection(String collectionName) {
		return jobStatistics.computeIfAbsent(collectionName, x -> new PersistedInstanceIdStatistics());
	}

	public boolean collectionNeedsCalculation(String collectionName) {
		if (!collectionIsDefinedInMirroredObjects(collectionName)) {
			log.warn("No mirrored object definition was found for collection [{}]", collectionName);
			return false;
		}
		try {
			List<IndexInfo> indices = spaceMirror.getDocumentDb().getCollection(collectionName).getIndexes().collect(toList());
			return getNumberOfPartitionsToCalculate().stream()
					.anyMatch(numPartitions -> indices.stream().noneMatch(isIndexForNumberOfPartitions(numPartitions)));
		} catch (Exception e) {
			log.warn("Could not determine whether persisted instance id should be calculated for collection [{}]", collectionName, e);
			return false;
		}
	}

	private boolean collectionIsDefinedInMirroredObjects(String collectionName) {
		return spaceMirror.getMirroredDocuments().stream()
				.map(MirroredObject::getCollectionName)
				.anyMatch(collectionName::equals);
	}

	@Override
	public void calculatePersistedInstanceId() {
		getCollectionsWithPersistInstanceIdEnabled()
				.forEach(collectionName -> calculatePersistedInstanceId(collectionName, getNumberOfPartitionsToCalculate()));
	}

	private Set<String> getCollectionsWithPersistInstanceIdEnabled() {
		return spaceMirror.getMirroredDocuments().stream()
				.filter(MirroredObject::persistInstanceId)
				.map(MirroredObject::getCollectionName)
				.collect(toSet());
	}

	private Set<Integer> getNumberOfPartitionsToCalculate() {
		int numberOfPartitions = determineNumberOfPartitions();
		Set<Integer> numberOfPartitionsSet = new HashSet<>();
		numberOfPartitionsSet.add(numberOfPartitions);
		ymerProperties.getNextNumberOfInstances().ifPresent(numberOfPartitionsSet::add);
		return numberOfPartitionsSet;
	}

	@Override
	public int[] getNumberOfPartitionsThatDataIsPreparedFor() {
		return getCollectionsWithPersistInstanceIdEnabled().stream()
				.map(this::getStatisticsForCollection)
				.map(PersistedInstanceIdStatistics::getReadyForNumberOfPartitionsSet)
				.reduce((a, b) -> {
					Set<Integer> intersection = new HashSet<>(a);
					intersection.retainAll(b);
					return intersection;
				})
				.orElse(emptySet())
				.stream().mapToInt(x -> x).sorted().toArray();
	}

	@Override
	public void calculatePersistedInstanceId(String collectionName) {
		if (!collectionIsDefinedInMirroredObjects(collectionName)) {
			log.warn("Cannot calculate instance id for collection [{}], no definition was found in mirrored objects",
					collectionName);
			return;
		}
		calculatePersistedInstanceId(collectionName, getNumberOfPartitionsToCalculate());
	}

	private Predicate<IndexInfo> isInstanceIdIndex() {
		return indexInfo -> indexInfo.getIndexFields().size() == 1 && indexInfo.getIndexFields().get(0).getKey().startsWith(DOCUMENT_INSTANCE_ID_PREFIX);
	}

	private void calculatePersistedInstanceId(String collectionName, Set<Integer> numberOfPartitionsSet) {
		log.info("Calculating instance id for collection {} with {} number of partitions and batch size {}",
				collectionName,
				numberOfPartitionsSet.stream().sorted().collect(toList()),
				BATCH_SIZE
		);
		DocumentCollection collection = spaceMirror.getDocumentDb().getCollection(collectionName);

		PersistedInstanceIdStatistics statistics = getStatisticsForCollection(collectionName);
		statistics.resetStatisticsForJobExecution(numberOfPartitionsSet);

		Set<String> fieldNamesToCalculate = numberOfPartitionsSet.stream()
				.map(PersistedInstanceIdUtil::getInstanceIdFieldName)
				.collect(toSet());

		List<IndexInfo> allInstanceIdIndexes = collection.getIndexes()
				.filter(isInstanceIdIndex())
				.collect(toList());

		Set<String> noLongerNeededFields = allInstanceIdIndexes.stream()
				.map(index -> index.getIndexFields().get(0).getKey())
				.filter(fieldName -> !fieldNamesToCalculate.contains(fieldName))
				.collect(toSet());

		boolean indexDropped = allInstanceIdIndexes.stream()
				.filter(index -> noLongerNeededFields.contains(index.getIndexFields().get(0).getKey()))
				.peek(index -> {
					log.info("Step 1/3\tDropping index {}", index.getName());
					collection.dropIndex(index.getName());
				})
				.findAny()
				.isPresent();
		if (!indexDropped) {
			log.info("Step 1/3\tNo index to drop");
		}

		log.info("Step 2/3\tWill calculate instance id for fields [{}]", String.join(", ", fieldNamesToCalculate));

		if (!noLongerNeededFields.isEmpty()) {
			log.info("Step 2/3\tWill delete no longer used fields [{}]", String.join(", ", noLongerNeededFields));
		}

		Query query = createQuery(BATCH_SIZE, fieldNamesToCalculate, noLongerNeededFields);

		try (Stream<List<Document>> batches = StreamUtils.buffer(collection.findByQuery(query), BATCH_SIZE)) {
			LongAdder analyzedCount = new LongAdder();
			AtomicInteger updatedCount = new AtomicInteger();
			batches.forEach(batch -> collection.bulkWrite(bulkWriter -> {
				numberOfPartitionsSet.forEach(numberOfPartitions -> {
					String fieldName = getInstanceIdFieldName(numberOfPartitions);
					Map<Integer, List<Document>> updatesByInstanceId = batch.stream()
							.collect(groupingBy(it -> getInstanceId(it.get(DOCUMENT_ROUTING_KEY), numberOfPartitions)));

					updatesByInstanceId.forEach((instanceId, documents) -> {
						Set<Object> ids = documents.stream()
								.peek(it -> analyzedCount.increment())
								.filter(document -> !Objects.equals(instanceId, document.get(fieldName)))
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
							bulkWriter.updatePartialByIds(ids, Map.of(fieldName, instanceId));
						}
					});

					noLongerNeededFields.forEach(noLongerNeededField -> {
						Set<Object> toDeleteFieldFor = batch.stream()
								.filter(document -> document.containsKey(noLongerNeededField))
								.map(document -> document.get("_id"))
								.filter(Objects::nonNull)
								.collect(Collectors.toSet());

						if (!toDeleteFieldFor.isEmpty()) {
							bulkWriter.unsetFieldsPartialByIds(toDeleteFieldFor, Set.of(noLongerNeededField));
						}
					});
				});
			}));
			log.info("Step 2/3\tUpdated persisted instance id for {} documents total ({} analyzed total)", updatedCount.get(), analyzedCount.sum());
		}

		numberOfPartitionsSet.forEach(numberOfPartitions -> {
			String fieldName = getInstanceIdFieldName(numberOfPartitions);
			boolean indexExists = collection.getIndexes()
					.anyMatch(isIndexForNumberOfPartitions(numberOfPartitions));
			if (indexExists) {
				log.info("Step 3/3\tIndex for field [{}] does not need to be created because it already exists", fieldName);
			} else {
				log.info("Step 3/3\tCreating index for field [{}]", fieldName);
				IndexOptions options = new IndexOptions().background(true);
				collection.createIndex(new Document(fieldName, 1), options);
				log.info("Step 3/3\tDone creating index");
			}
		});

		statistics.calculationCompleted(numberOfPartitionsSet);
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
		return GigaSpacesInstanceIdUtil.getNumberOfPartitionsFromSpaceProperties(applicationContext)
				.map(peek(numberOfPartitions ->
								  log.info("Using {} number of partitions (from space property \"{}\")", numberOfPartitions, MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT)
				));
	}

	private Optional<Integer> getNumberOfPartitionsFromSystemProperty() {
		return GigaSpacesInstanceIdUtil.getNumberOfPartitionsFromSystemProperty()
				.map(peek(numberOfPartitions ->
								  log.info("Using {} number of partitions (from system property \"{}\")", numberOfPartitions, NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY)
				));
	}

	@SuppressWarnings("SameParameterValue")
	private Query createQuery(int batchSize, Set<String> fieldsToSet, Set<String> fieldsToRemove) {
		List<Criteria> orCriteria = new ArrayList<>();
		for (String field : fieldsToSet) {
			orCriteria.add(where(field).exists(false));
		}
		for (String field : fieldsToRemove) {
			orCriteria.add(where(field).exists(true));
		}
		Criteria fieldCriteria = new Criteria().orOperator(orCriteria.toArray(new Criteria[0]));

		Query query = query(where(DOCUMENT_ROUTING_KEY).exists(true).andOperator(fieldCriteria));
		query.fields().include(DOCUMENT_ROUTING_KEY);
		fieldsToSet.forEach(field -> query.fields().include(field));
		fieldsToRemove.forEach(field -> query.fields().include(field));
		return query.cursorBatchSize(batchSize);
	}

	private static <T> UnaryOperator<T> peek(Consumer<T> consumer) {
		return it -> {
			consumer.accept(it);
			return it;
		};
	}

}
