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

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

class PerMinuteCounter {

	private final Map<Long, List<Long>> map = new ConcurrentHashMap<>();
	private final Clock clock;

	PerMinuteCounter() {
		this(Clock.systemDefaultZone());
	}

	PerMinuteCounter(Clock clock) {
		this.clock = clock;
	}

	public void addPerMinuteCount(long value) {
		Long currentTime = getLocalDateTime();
		map.computeIfAbsent(currentTime, m -> new CopyOnWriteArrayList<>()).add(value);
		map.keySet().stream()
				.filter(k -> currentTime - k > 60_000)
				.forEach(map::remove);
	}

	private long getLocalDateTime() {
		return LocalDateTime.now(clock).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	public long getCurrentMinuteSum() {
		Long currentTime = getLocalDateTime();
		return map.entrySet().stream()
				.filter(e -> currentTime - e.getKey() < 60_000)
				.flatMapToLong(e -> e.getValue().stream().mapToLong(Long::longValue))
				.sum();
	}

	public long getCurrentMinuteRate() {
		Long currentTime = getLocalDateTime();
		return map.entrySet().stream()
				.filter(e -> currentTime - e.getKey() < 60_000)
				.mapToLong(e -> e.getValue().size())
				.sum();
	}

	/**
	 * open for testing
	 */
	int getMapSize() {
		return map.size();
	}

}