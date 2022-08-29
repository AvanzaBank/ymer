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

import java.util.concurrent.atomic.AtomicLong;

public class PerformedOperationMetrics implements PerformedOperationMetricsMBean, PerformedOperationsListener {

	private final AtomicLong numInserts = new AtomicLong();
	private final AtomicLong numUpdates = new AtomicLong();
	private final AtomicLong numDeletes = new AtomicLong();

	public long getNumPerformedOperations() {
		return numInserts.get() + numUpdates.get() + numDeletes.get();
	}

	public long getNumInserts() {
		return numInserts.get();
	}

	public long getNumUpdates() {
		return numUpdates.get();
	}

	public long getNumDeletes() {
		return numDeletes.get();
	}

	@Override
	public void increment(OperationType type, int delta) {
		switch (type) {
			case INSERT:
				numInserts.addAndGet(delta);
				break;
			case UPDATE:
				numUpdates.addAndGet(delta);
				break;
			case DELETE:
				numDeletes.addAndGet(delta);
				break;
		}
	}
}
