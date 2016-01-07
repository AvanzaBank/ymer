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

import javax.annotation.PostConstruct;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;

import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

final class VersionedMongoSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint implements ClusterInfoAware {
	
	private final VersionedMongoDBExternalDataSource target;
	
	public VersionedMongoSpaceSynchronizationEndpoint(VersionedMongoDBExternalDataSource target) {
		this.target = target;
	}

	@Override
	public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
		target.executeBulk(batchData);
	}
	
	@PostConstruct
	public void registerExceptionHandlerMBean() {
		target.registerExceptionHandlerMBean();
	}

	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		target.setClusterInfo(clusterInfo);
	}
	
}
