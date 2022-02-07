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
package com.avanza.ymer.util;

import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT;

import java.util.Optional;

import org.springframework.context.ApplicationContext;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.IJSpace;

/**
 * @see com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils
 */
public final class GigaSpacesInstanceIdUtil {

	public static final String NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY = "cluster.partitions";

	private GigaSpacesInstanceIdUtil() {
	}

	public static int getInstanceId(Object routingKey, int partitionCount) {
		return safeAbsoluteValue(routingKey.hashCode()) % partitionCount + 1;
	}

	private static int safeAbsoluteValue(int value) {
		return value == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(value);
	}

	public static Optional<Integer> getNumberOfPartitionsFromSpaceProperties(ApplicationContext applicationContext) {
		return Optional.ofNullable(applicationContext)
				.map(it -> it.getBeanProvider(IJSpace.class).getIfAvailable())
				.map(IJSpace::getDirectProxy)
				.map(IDirectSpaceProxy::getSpaceImplIfEmbedded)
				.map(SpaceImpl::getConfigReader)
				.map(it -> it.getIntSpaceProperty(MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, "0"))
				.filter(it -> it > 0);
	}

	public static Optional<Integer> getNumberOfPartitionsFromSystemProperty() {
		return Optional.ofNullable(System.getProperty(NUMBER_OF_PARTITIONS_SYSTEM_PROPERTY))
				.map(Integer::valueOf);
	}

}
