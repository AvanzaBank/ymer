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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import org.openspaces.core.cluster.ClusterInfo;

import java.util.Collection;

public abstract class AbstractSpaceDataSourceDecorator extends AbstractSpaceDataSource {

	private final AbstractSpaceDataSource abstractSpaceDataSource;

	public AbstractSpaceDataSourceDecorator(AbstractSpaceDataSource abstractSpaceDataSource) {
		this.abstractSpaceDataSource = abstractSpaceDataSource;
	}

	@Override
	public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
		return abstractSpaceDataSource.initialMetadataLoad();
	}

	@Override
	public DataIterator<Object> initialDataLoad() {
		return abstractSpaceDataSource.initialDataLoad();
	}

	@Override
	public DataIterator<Object> getDataIterator(DataSourceQuery query) {
		return abstractSpaceDataSource.getDataIterator(query);
	}

	@Override
	public Object getById(DataSourceIdQuery idQuery) {
		return abstractSpaceDataSource.getById(idQuery);
	}

	@Override
	public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
		return abstractSpaceDataSource.getDataIteratorByIds(idsQuery);
	}

	@Override
	public boolean supportsInheritance() {
		return abstractSpaceDataSource.supportsInheritance();
	}

	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		abstractSpaceDataSource.setClusterInfo(clusterInfo);
	}

	@Override
	public <T> T loadObject(Class<T> spaceType, Object documentId) {
		return abstractSpaceDataSource.loadObject(spaceType, documentId);
	}

	@Override
	public <T> Collection<T> loadObjects(Class<T> aClass, T t) {
		return abstractSpaceDataSource.loadObjects(aClass, t);
	}

}
