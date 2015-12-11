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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

public class CompositeManagedDataSourceTest {

	private class MyMDS implements ManagedDataSource<Object> {
		boolean initCalled = false;
		boolean initialLoadCalled = false;
		boolean shutdownCalled = false;

		@Override
		public void init(Properties arg0) throws DataSourceException {
			initCalled = true;
		}

		@Override
		public DataIterator<Object> initialLoad() throws DataSourceException {
			initialLoadCalled = true;
			return new DataIterator<Object>() {
				@Override public boolean hasNext() { return false; }
				@Override public Object next() { return null; }
				@Override public void remove() { }
				@Override public void close() { }
			};
		}

		@Override
		public void shutdown() throws DataSourceException {
			shutdownCalled = true;
		}
	}

	private class MyMDSCIA extends MyMDS implements ClusterInfoAware {
		boolean setClusterInfoCalled = false;

		@Override
		public void setClusterInfo(ClusterInfo clusterInfo) {
			setClusterInfoCalled = true;
		}

	}

	MyMDS mds = new MyMDS();
	MyMDSCIA mdsCia = new MyMDSCIA();
	@SuppressWarnings("unchecked")
	CompositeManagedDataSource compositeManagedDataSource = CompositeManagedDataSource.create(mds, mdsCia);

	@Test
	public void initIsCalledForAllManagesDataSources() throws Exception {
		assertFalse(mds.initCalled);
		assertFalse(mdsCia.initCalled);
		compositeManagedDataSource.init(null);
		assertTrue(mds.initCalled);
		assertTrue(mdsCia.initCalled);
	}

	@Test
	public void initialLoadIsCalledForAllManagesDataSources() throws Exception {
		assertFalse(mds.initialLoadCalled);
		assertFalse(mdsCia.initialLoadCalled);
		compositeManagedDataSource.initialLoad();
		assertTrue(mds.initialLoadCalled);
		assertTrue(mdsCia.initialLoadCalled);
	}

	@Test
	public void shutdownIsCalledForAllManagesDataSources() throws Exception {
		assertFalse(mds.shutdownCalled);
		assertFalse(mdsCia.shutdownCalled);
		compositeManagedDataSource.shutdown();
		assertTrue(mds.shutdownCalled);
		assertTrue(mdsCia.shutdownCalled);
	}

	@Test
	public void setClusterInfoIsCalledForAllClusterInfoAware() throws Exception {
		assertFalse(mdsCia.setClusterInfoCalled);
		compositeManagedDataSource.setClusterInfo(null);
		assertTrue(mdsCia.setClusterInfoCalled);
	}

}
