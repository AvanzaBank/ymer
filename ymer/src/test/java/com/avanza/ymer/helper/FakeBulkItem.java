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
package com.avanza.ymer.helper;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.DataSyncOperationType;

public class FakeBulkItem implements DataSyncOperation {

	private final Object item;
	private final DataSyncOperationType operation;

	public FakeBulkItem(Object item, DataSyncOperationType operation) {
		this.item = item;
		this.operation = operation;
	}

	@Override
	public Object getDataAsObject() {
		return this.item;
	}

	@Override
	public DataSyncOperationType getDataSyncOperationType() {
		return operation;
	}

	@Override
	public Object getSpaceId() {
		return null;
	}

	@Override
	public SpaceTypeDescriptor getTypeDescriptor() {
		return null;
	}

	@Override
	public String getUid() {
		return null;
	}

	@Override
	public boolean supportsDataAsDocument() {
		return false;
	}

	@Override
	public boolean supportsDataAsObject() {
		return false;
	}

	@Override
	public boolean supportsGetSpaceId() {
		return false;
	}

	@Override
	public boolean supportsGetTypeDescriptor() {
		return false;
	}

	@Override
	public SpaceDocument getDataAsDocument() {
		return null;
	}
}
