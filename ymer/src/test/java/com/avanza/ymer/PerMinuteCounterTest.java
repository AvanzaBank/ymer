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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.junit.Test;
import org.mockito.Mockito;

public class PerMinuteCounterTest {

	@Test
	public void shouldCalculateSumAndRateForSameExactMinute() {
		final PerMinuteCounter perMinuteCounter = new PerMinuteCounter(Clock.fixed(Instant.now(), ZoneId.systemDefault()));
		perMinuteCounter.addPerMinuteCount(100);

		assertEquals(100, perMinuteCounter.getCurrentMinuteSum());
		assertEquals(1, perMinuteCounter.getCurrentMinuteRate());

		perMinuteCounter.addPerMinuteCount(100);

		assertEquals(200, perMinuteCounter.getCurrentMinuteSum());
		assertEquals(2, perMinuteCounter.getCurrentMinuteRate());

		assertEquals(1, perMinuteCounter.getMapSize());

	}

	@Test
	public void shouldCalculateSumAndRateCallsDuringForSameMinute() {
		Instant now = Instant.now().truncatedTo(ChronoUnit.MINUTES);
		Clock c = Mockito.spy(Clock.systemDefaultZone());
		Mockito.when(c.instant()).thenReturn(now, now.plusSeconds(20), now.plusSeconds(40));

		final PerMinuteCounter perMinuteCounter = new PerMinuteCounter(c);
		perMinuteCounter.addPerMinuteCount(100);
		perMinuteCounter.addPerMinuteCount(200);
		perMinuteCounter.addPerMinuteCount(300);

		assertEquals(600, perMinuteCounter.getCurrentMinuteSum());
		assertEquals(3, perMinuteCounter.getCurrentMinuteRate());

		assertEquals(3, perMinuteCounter.getMapSize());

	}

	@Test
	public void shouldCalculateSumAndRateForDifferentMinute() {
		Instant now = Instant.now();
		Clock c = Mockito.spy(Clock.systemDefaultZone());
		Mockito.when(c.instant()).thenReturn(now, now.plusSeconds(61));

		final PerMinuteCounter perMinuteCounter = new PerMinuteCounter(c);
		perMinuteCounter.addPerMinuteCount(100);
		perMinuteCounter.addPerMinuteCount(200);

		assertEquals(200, perMinuteCounter.getCurrentMinuteSum());
		assertEquals(1, perMinuteCounter.getCurrentMinuteRate());

		assertEquals(1, perMinuteCounter.getMapSize());

	}
}