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
package se.avanzabank.mongodb.support.mirror;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;

import se.avanzabank.mongodb.support.IteratorIterator;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

/**
 * Allows grouping of several {@link ManagedDataSource} into one so that we support different kinds of source for InitialLoad
 * @author joasah Joakim Sahlström
 */
public class CompositeManagedDataSource implements ManagedDataSource<Object>, ClusterInfoAware {

	private final Collection<ManagedDataSource<Object>> dataSources;

	protected CompositeManagedDataSource(Collection<ManagedDataSource<Object>> dataSources) {
		this.dataSources = Collections.unmodifiableList(new ArrayList<>(dataSources));
	}

	/**
	 * Create a new instance of {@link CompositeManagedDataSource}
	 * @param dataSources {@link ManagedDataSource}s
	 * @return a new {@link CompositeManagedDataSource} instance
	 */
	@SafeVarargs
	public static CompositeManagedDataSource create(ManagedDataSource<Object>... dataSources) {
		return new CompositeManagedDataSource(Arrays.asList(dataSources));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void init(Properties properties) throws DataSourceException {
		for (ManagedDataSource<Object> dataSource : dataSources) {
			dataSource.init(properties);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DataIterator<Object> initialLoad() throws DataSourceException {
		ArrayList<Iterator<Object>> iterators = new ArrayList<>();
		for (ManagedDataSource<Object> dataSource : dataSources) {
			iterators.add(dataSource.initialLoad());
		}
		return new IteratorIterator<Object>(iterators);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() throws DataSourceException {
		for (ManagedDataSource<Object> dataSource : dataSources) {
			dataSource.shutdown();
		}
	}

	// ----- From ClusterInfoAware -----

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		for (ManagedDataSource<Object> dataSource : dataSources) {
			if (dataSource instanceof ClusterInfoAware) {
				ClusterInfoAware cia = (ClusterInfoAware) dataSource;
				cia.setClusterInfo(clusterInfo);
			}
		}
	}

}
