package com.avanza.ymer.util;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.util.LinkedList;
import java.util.List;
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
}