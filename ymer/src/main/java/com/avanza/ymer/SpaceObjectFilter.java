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

import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;

import java.util.Objects;

/**
 * Strategy for filtering out objects during initial load. <p>
 *
 * The production implementation accepts SpaceObject's routed to the current partition. <p>
 *
 * @author Elias Lindholm (elilin)
 *
 * @param <T>
 */
final class SpaceObjectFilter<T> {

	private final Impl<T> impl;

	private SpaceObjectFilter(Impl<T> impl) {
		this.impl = Objects.requireNonNull(impl);
	}

	static <T> SpaceObjectFilter<T> partitionFilter(MirroredObject<T> document, int instanceId, int partitionCount) {
		return new SpaceObjectFilter<>(new PartitionFilter<>(document, instanceId, partitionCount));
	}

	static <T> SpaceObjectFilter<T> create(Impl<T> impl) {
		return new SpaceObjectFilter<>(impl);
	}

	static <T> SpaceObjectFilter<T> acceptAll() {
		return new SpaceObjectFilter<>(o -> true);
	}

	boolean accept(T spaceObject) {
		return this.impl.accept(spaceObject);
	}

	interface Impl<T> {
		boolean accept(T spaceObject);
	}

	public boolean hasPartitionFilter() {
		return impl instanceof PartitionFilter;
	}

	public PartitionFilter<T> getPartitionFilter() {
		return (PartitionFilter<T>) impl;
	}

	public static class PartitionFilter<T> implements SpaceObjectFilter.Impl<T> {

		private final MirroredObject<T> document;
		private final int instanceId;
		private final int partitionCount;

		public PartitionFilter(MirroredObject<T> document, int instanceId, int partitionCount) {
			this.document = document;
			this.instanceId = instanceId;
			this.partitionCount = partitionCount;
		}

		@Override
		public boolean accept(T spaceObject) {
			return isRoutedToThisPartition(spaceObject);
		}

		private boolean isRoutedToThisPartition(T spaceObject) {
			Object routingKey = getRoutingKey(spaceObject);
			if (routingKey == null) {
				throw new RuntimeException("Routing key was null for space object " + spaceObject);
			}
			return routesToThisPartition(routingKey);
		}

		private Object getRoutingKey(T spaceObject) {
			return this.document.getRoutingKey(spaceObject);
		}

		private boolean routesToThisPartition(Object routingKey) {
			return instanceId == getInstanceId(routingKey, partitionCount);
		}

		public int getTotalPartitions() {
			return partitionCount;
		}

		public int getCurrentPartition() {
			return instanceId;
		}
	}

}
