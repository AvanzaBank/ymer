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