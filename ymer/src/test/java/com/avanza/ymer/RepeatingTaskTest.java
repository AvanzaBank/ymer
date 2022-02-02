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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;

public class RepeatingTaskTest {

	@Test
	public void shouldTriggerExpectedNumberOfTimes() throws Exception {
		LongAdder counter = new LongAdder();

		try(RepeatingTask ignore = new RepeatingTask(Duration.ofMillis(5), Duration.ofMillis(10), counter::increment)) {
			Thread.sleep(100);
		}

		assertThat(counter.sum(), is(10L));
	}

	@Test
	public void shouldNotTriggerAfterClosed() throws Exception {
		LongAdder counter = new LongAdder();

		try(RepeatingTask ignore = new RepeatingTask(Duration.ZERO, Duration.ofMillis(100), counter::increment)) {
			Thread.sleep(5);
			assumeTrue(counter.sum() == 1L);
		}

		Thread.sleep(10);
		assertThat(counter.sum(), is(1L));
	}

}