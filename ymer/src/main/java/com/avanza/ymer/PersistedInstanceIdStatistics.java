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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openspaces.core.util.ConcurrentHashSet;

class PersistedInstanceIdStatistics implements PersistedInstanceIdStatisticsMBean {

	private final Set<Integer> readyForNumberOfPartitionsSet = new ConcurrentHashSet<>();
	private final AtomicBoolean calculationInProgress = new AtomicBoolean(false);

	public void resetStatisticsForJobExecution(Set<Integer> calculatingForPartitions) {
		readyForNumberOfPartitionsSet.removeIf((numberOfPartitions -> !calculatingForPartitions.contains(numberOfPartitions)));
		calculationInProgress.set(true);
	}

	public void addReadyForNumberOfPartitions(int numberOfPartitions) {
		readyForNumberOfPartitionsSet.add(numberOfPartitions);
	}

	public void calculationCompleted(Set<Integer> calculatingForPartitions) {
		calculatingForPartitions.forEach(this::addReadyForNumberOfPartitions);
		calculationInProgress.set(false);
	}

	public Set<Integer> getReadyForNumberOfPartitionsSet() {
		return readyForNumberOfPartitionsSet;
	}

	@Override
	public int[] getReadyForNumberOfPartitions() {
		return readyForNumberOfPartitionsSet.stream().mapToInt(x -> x).sorted().toArray();
	}

	@Override
	public boolean isCalculationInProgress() {
		return calculationInProgress.get();
	}
}
