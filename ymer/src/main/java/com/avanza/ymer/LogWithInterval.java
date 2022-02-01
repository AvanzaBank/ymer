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
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility for logging once in every time interval. The timer starts when {@link #shouldLog()} is called the first time.
 * Intended to be used to show progress of data loading etc. without clogging the logs.
 */
class LogWithInterval {

	private final Duration interval;
	private final AtomicReference<Instant> nextLogTime = new AtomicReference<>(null);
	private Clock clock = Clock.systemDefaultZone();

	public LogWithInterval(Duration interval) {
		this.interval = interval;
	}

	// Visible for testing
	void setClock(Clock clock) {
		this.clock = clock;
	}

	public boolean shouldLog() {
		Instant nextTime = nextLogTime.get();
		Instant now = clock.instant();
		if (nextTime == null) {
			nextLogTime.set(now.plus(interval));
			return false;
		} else if (now.isAfter(nextTime)) {
			return nextLogTime.compareAndSet(nextTime, now.plus(interval));
		}
		return false;
	}

}
