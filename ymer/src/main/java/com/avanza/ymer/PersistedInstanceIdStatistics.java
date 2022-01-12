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
import java.util.concurrent.atomic.LongAdder;

import org.openspaces.core.util.ConcurrentHashSet;

class PersistedInstanceIdStatistics implements PersistedInstanceIdStatisticsMBean {

	private final Set<Integer> readyForNumberOfPartitions = new ConcurrentHashSet<>();
	private final LongAdder calculationProgress = new LongAdder();
	private Long totalToCalculate;

	public void resetStatisticsForJobExecution(Set<Integer> calculatingForPartitions) {
		calculationProgress.reset();
		totalToCalculate = 0L;
		readyForNumberOfPartitions.removeIf((numberOfPartitions -> !calculatingForPartitions.contains(numberOfPartitions)));
	}

	public void addToCalculationProgress(Integer calculationProgress) {
		this.calculationProgress.add(calculationProgress);
	}

	public void setTotalToCalculate(Long totalToCalculate) {
		this.totalToCalculate = totalToCalculate;
	}

	public void addReadyForNumberOfPartitions(int numberOfPartitions) {
		readyForNumberOfPartitions.add(numberOfPartitions);
	}

	@Override
	public int[] getReadyForNumberOfPartitions() {
		return readyForNumberOfPartitions.stream().mapToInt(x -> x).sorted().toArray();
	}

	@Override
	public Long getCalculationProgress() {
		return calculationProgress.longValue();
	}

	@Override
	public Long getTotalToCalculate() {
		return totalToCalculate;
	}
}
