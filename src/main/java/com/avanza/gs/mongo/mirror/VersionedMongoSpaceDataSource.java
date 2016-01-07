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

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.springframework.beans.factory.annotation.Autowired;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.SpaceDataSource;

final class VersionedMongoSpaceDataSource extends SpaceDataSource implements ClusterInfoAware, SpaceObjectLoader {

	private final VersionedMongoDBExternalDataSource ds;
	
	@Autowired
	public VersionedMongoSpaceDataSource(VersionedMongoDBExternalDataSource ds) {
		this.ds = ds;
	}
	
	@Override
	public DataIterator<Object> initialDataLoad() {
		return ds.initialLoad();
	}

	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		ds.setClusterInfo(clusterInfo);
	}

	@Override
	public <T extends ReloadableSpaceObject> T reloadObject(Class<T> spaceType, Object documentId) {
		return ds.reloadObject(spaceType, documentId);
	}

	@Override
	public <T> Collection<T> loadObjects(Class<T> spaceType, T template) {
		return ds.loadObjects(spaceType, template);
	}
	
	
	
}
