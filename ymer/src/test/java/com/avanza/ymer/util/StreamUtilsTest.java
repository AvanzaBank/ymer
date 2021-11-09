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
package com.avanza.ymer.util;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;

public class StreamUtilsTest {

	@Test
	public void shouldReturnAllElementsInBatches() {
		LongAdder numberOfBatches = new LongAdder();
		List<Integer> totalResults = new LinkedList<>();

		StreamUtils.buffer(IntStream.rangeClosed(1, 33).boxed(), 10)
				.peek(ignore -> numberOfBatches.increment())
				.forEach(totalResults::addAll);

		assertThat(numberOfBatches.intValue(), is(4));
		assertThat(totalResults, is(IntStream.rangeClosed(1, 33).boxed().collect(toList())));
	}

	@Test
	public void shouldReturnFirstTwoBatches() {
		LongAdder numberOfBatches = new LongAdder();
		List<Integer> totalResults = new LinkedList<>();

		StreamUtils.buffer(IntStream.rangeClosed(1, 33).boxed(), 10)
				.limit(2)
				.peek(ignore -> numberOfBatches.increment())
				.forEach(totalResults::addAll);

		assertThat(numberOfBatches.intValue(), is(2));
		assertThat(totalResults, is(IntStream.rangeClosed(1, 20).boxed().collect(toList())));
	}

	@Test
	public void shouldReturnEmptyStream() {
		LongAdder numberOfBatches = new LongAdder();
		List<Integer> totalResults = new LinkedList<>();

		StreamUtils.buffer(Stream.<Integer>empty(), 10)
				.peek(ignore -> numberOfBatches.increment())
				.forEach(totalResults::addAll);

		assertThat(numberOfBatches.intValue(), is(0));
		assertThat(totalResults, is(emptyList()));
	}

	@Test
	public void shouldConsumeSourceStreamOnTerminalOperation() {
		Stream<Integer> source = Stream.of(1);

		StreamUtils.buffer(source, 10).forEach(it -> {});

		assertThrows("Stream should already be consumed", IllegalStateException.class, () -> source.forEach(it -> {}));
	}

	@Test
	@SuppressWarnings("ResultOfMethodCallIgnored")
	public void shouldNotConsumeSourceStreamOnNoTerminalOperation() {
		Stream<Integer> source = Stream.of(1);

		StreamUtils.buffer(source, 10).map(List::size);

		source.forEach(it -> {});
	}

	@Test
	public void closingBufferedStreamShouldCloseSourceStream() {
		AtomicBoolean sourceClosed = new AtomicBoolean(false);
		Stream<Integer> source = Stream.of(1).onClose(() -> sourceClosed.set(true));

		Stream<List<Integer>> bufferingStream = StreamUtils.buffer(source, 1);

		bufferingStream.close();

		assertThat(sourceClosed.get(), is(true));
	}
}