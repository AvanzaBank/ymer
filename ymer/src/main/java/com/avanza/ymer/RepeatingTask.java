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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

class RepeatingTask implements AutoCloseable{
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	private final ScheduledFuture<?> task;

	public RepeatingTask(Duration initialDelay, Duration fixedRate, Runnable action) {
		task = executor.scheduleAtFixedRate(action, initialDelay.toMillis(), fixedRate.toMillis(), MILLISECONDS);
	}

	public RepeatingTask(Duration initialDelayAndFixedRate, Runnable action) {
		this(initialDelayAndFixedRate, initialDelayAndFixedRate, action);
	}

	@Override
	public void close() {
		task.cancel(true);
		executor.shutdown();
	}

}
