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

import java.util.concurrent.atomic.LongAdder;

public class PerformedOperationMetrics implements PerformedOperationMetricsMBean, PerformedOperationsListener {

	private final LongAdder numInserts = new LongAdder();
	private final LongAdder numUpdates = new LongAdder();
	private final LongAdder numDeletes = new LongAdder();
	private final LongAdder numFailures = new LongAdder();

	private final PerMinuteCounter batchSizePerMinute = new PerMinuteCounter();

	public long getNumPerformedOperations() {
		return getNumInserts() + getNumUpdates() + getNumDeletes();
	}

	@Override
	public long getNumInserts() {
		return numInserts.sum();
	}

	@Override
	public long getNumUpdates() {
		return numUpdates.sum();
	}

	@Override
	public long getNumDeletes() {
		return numDeletes.sum();
	}

	@Override
	public long getNumFailures() {
		return numFailures.sum();
	}

	@Override
	public long getBatchReadRate() {
		return batchSizePerMinute.getCurrentMinuteSum() / Math.max(1, batchSizePerMinute.getCurrentMinuteRate());
	}

	@Override
	public void increment(OperationType type, int delta) {
		switch (type) {
			case INSERT:
				numInserts.add(delta);
				break;
			case UPDATE:
				numUpdates.add(delta);
				break;
			case DELETE:
				numDeletes.add(delta);
				break;
			case FAILURE:
				numFailures.add(delta);
				break;
			case READ_BATCH:
				batchSizePerMinute.addPerMinuteCount(delta);
				break;
		}
	}

}