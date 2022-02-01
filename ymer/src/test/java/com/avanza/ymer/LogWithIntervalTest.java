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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

import org.junit.Test;

public class LogWithIntervalTest {

	@Test
	public void testLogWithInterval() {
		LogWithInterval timedLogger = new LogWithInterval(Duration.ofSeconds(30));

		Instant now = Instant.now();
		timedLogger.setClock(Clock.fixed(now, ZoneId.systemDefault()));

		assertFalse("First call should start the timer and return false", timedLogger.shouldLog());
		assertFalse("Without waiting, the next call should also return false", timedLogger.shouldLog());

		timedLogger.setClock(Clock.fixed(now.plusSeconds(31), ZoneId.systemDefault()));

		assertTrue("After waiting 31 seconds, should return true", timedLogger.shouldLog());
		assertFalse("The timer should now be updated and subsequent calls should return false", timedLogger.shouldLog());
	}

	@Test
	public void shouldOnlyLogOnceEveryIntervalWithThreads() throws InterruptedException {
		LogWithInterval timedLogger = new LogWithInterval(Duration.ofMillis(100));
		LongAdder longAdder = new LongAdder();

		ExecutorService service = Executors.newFixedThreadPool(4);
		Instant testStart = Instant.now();

		IntStream.range(0, 4).forEach(i -> service.submit(() -> {
			while (Instant.now().isBefore(testStart.plusSeconds(1))) {
				if (timedLogger.shouldLog()) {
					longAdder.increment();
				}
				try {
					TimeUnit.MILLISECONDS.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}));
		service.shutdown();
		service.awaitTermination(2, TimeUnit.SECONDS);

		assertThat(longAdder.intValue(), is(9));
	}
}
