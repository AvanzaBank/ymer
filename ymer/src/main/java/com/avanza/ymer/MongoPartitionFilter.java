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
import java.util.Objects;
import java.util.stream.IntStream;

import com.avanza.ymer.SpaceObjectFilter.PartitionFilter;
import com.mongodb.BasicDBObject;

class MongoPartitionFilter {

	private final BasicDBObject filter;

	private static boolean loadByShard = true;
	
	public MongoPartitionFilter(BasicDBObject filter) {
		this.filter = Objects.requireNonNull(filter);
	}

	public static MongoPartitionFilter create(SpaceObjectFilter<?> spaceObjectFilter) {
		return new MongoPartitionFilter(buildFilter(spaceObjectFilter.getPartitionFilter()));
	}

	public static boolean canCreateFrom(SpaceObjectFilter<?> spaceObjectFilter) {
		return spaceObjectFilter.hasPartitionFilter();
	}

	private static BasicDBObject buildFilter(PartitionFilter<?> partitionFilter) {
		if (loadByShard) {
			int maxShards = 1024;
			int[] shards = IntStream.range(0, maxShards).filter(i -> i%partitionFilter.getCurrentPartition() == 0).toArray();
			return new BasicDBObject("_shard", new BasicDBObject("$in", shards));
		} else {
			return new BasicDBObject("$or", Arrays.asList(
				new BasicDBObject(MirroredObject.DOCUMENT_ROUTING_KEY, new BasicDBObject("$mod", Arrays.asList(partitionFilter.getTotalPartitions(), partitionFilter.getCurrentPartition() - 1))),
				new BasicDBObject(MirroredObject.DOCUMENT_ROUTING_KEY, new BasicDBObject("$mod", Arrays.asList(partitionFilter.getTotalPartitions(), -(partitionFilter.getCurrentPartition() - 1)))),
				new BasicDBObject(MirroredObject.DOCUMENT_ROUTING_KEY, new BasicDBObject("$exists", false))
				));
		}
	}

	public BasicDBObject toDBObject() {
		return filter;
	}

	@Override
	public String toString() {
		return filter.toString();
	}

}
