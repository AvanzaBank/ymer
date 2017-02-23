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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;

public class AbstractSpaceDataSourceDecoratorTest {

	private AbstractSpaceDataSource mock;
	private AbstractSpaceDataSourceDecoratorImpl target;

	@Before
	public void setUp() throws Exception {
		mock = mock(AbstractSpaceDataSource.class);
		target = new AbstractSpaceDataSourceDecoratorImpl(mock);
	}

	@Test
	public void testInitialMetadataLoad() throws Exception {
		target.initialMetadataLoad();

		verify(mock, times(1)).initialMetadataLoad();
	}

	@Test
	public void testInitialDataLoad() throws Exception {
		target.initialDataLoad();

		verify(mock, times(1)).initialDataLoad();
	}

	@Test
	public void testGetDataIterator() throws Exception {
		DataSourceQuery dataSourceQuery = mock(DataSourceQuery.class);
		target.getDataIterator(dataSourceQuery);

		verify(this.mock, times(1)).getDataIterator(dataSourceQuery);
	}

	@Test
	public void testGetById() throws Exception {
		DataSourceIdQuery dataSourceIdQuery = mock(DataSourceIdQuery.class);
		target.getById(dataSourceIdQuery);

		verify(this.mock, times(1)).getById(dataSourceIdQuery);
	}

	@Test
	public void testGetDataIteratorByIds() throws Exception {
		DataSourceIdsQuery dataSourceIdQuery = mock(DataSourceIdsQuery.class);
		target.getDataIteratorByIds(dataSourceIdQuery);

		verify(mock, times(1)).getDataIteratorByIds(dataSourceIdQuery);
	}

	@Test
	public void testSupportsInheritance() throws Exception {
		target.supportsInheritance();

		verify(mock, times(1)).supportsInheritance();
	}

	@Test
	public void testSetClusterInfo() throws Exception {
		ClusterInfo clusterInfo = mock(ClusterInfo.class);
		target.setClusterInfo(clusterInfo);

		verify(this.mock, times(1)).setClusterInfo(clusterInfo);
	}

	@Test
	public void testReloadObject() throws Exception {
		ReloadableSpaceObject reloadableSpaceObject = mock(ReloadableSpaceObject.class);
		target.reloadObject(reloadableSpaceObject.getClass(), reloadableSpaceObject);

		verify(this.mock, times(1)).reloadObject(reloadableSpaceObject.getClass(), reloadableSpaceObject);
	}

	@Test
	public void testLoadObjects() throws Exception {
		String string = "test";
		target.loadObjects(String.class, string);

		verify(mock, times(1)).loadObjects(String.class, string);
	}

	public class AbstractSpaceDataSourceDecoratorImpl extends AbstractSpaceDataSourceDecorator {
		public AbstractSpaceDataSourceDecoratorImpl(AbstractSpaceDataSource abstractSpaceDataSource) {
			super(abstractSpaceDataSource);
		}
	}

}