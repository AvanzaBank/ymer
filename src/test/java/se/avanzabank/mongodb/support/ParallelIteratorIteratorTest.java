package se.avanzabank.mongodb.support;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class ParallelIteratorIteratorTest {

	@Test
	public void shouldLoadLargeNumberOfElements() throws Exception {

		List<Iterator<String>> iterators = new ArrayList<>();
		int numElements = 1000;
		int numBatches = 25;
		for (int i = 0; i < numBatches; i++) {
			iterators.add(defaultIterator(numElements, i));
		}

		ParallelIteratorIterator<String> iterator = new ParallelIteratorIterator<>(iterators);
		int count = 0;
		Map<String, AtomicInteger> map = new HashMap<>();
		while (iterator.hasNext()) {
			String val = iterator.next();
			map.computeIfAbsent(val, v -> new AtomicInteger()).incrementAndGet();
			count++;
		}
		assertEquals(numBatches * numElements, count);
		assertEquals(numBatches, map.keySet().size());
		for (AtomicInteger i : map.values()) {
			assertEquals(numElements, i.get());
		}
	}

	private Iterator<String> defaultIterator(int numElements, int i) {
		return Collections.nCopies(numElements, "foo" + i).iterator();
	}

	@Test(expected=TestException.class)
	public void shouldAbortIfOneProducerFails() throws Exception {

		List<Iterator<String>> iterators = Arrays.asList(defaultIterator(100, 0), new ErrorThrowingProducer(10),
				defaultIterator(100, 1));

		Map<String, AtomicInteger> map = new HashMap<>();
		ParallelIteratorIterator<String> iterator = new ParallelIteratorIterator<>(iterators);
		while (iterator.hasNext()) {
			String val = iterator.next();
			map.computeIfAbsent(val, v -> new AtomicInteger()).incrementAndGet();
		}
	}

	@Test
	public void shouldLoadZeroElementsWithSlowProducer() throws Exception {

		List<Iterator<String>> iterators = Arrays.asList(new SlowProducer(200));

		int count = 0;
		Map<String, AtomicInteger> map = new HashMap<>();
		ParallelIteratorIterator<String> iterator = new ParallelIteratorIterator<>(iterators);
		while (iterator.hasNext()) {
			String val = iterator.next();
			map.computeIfAbsent(val, v -> new AtomicInteger()).incrementAndGet();
			count++;
		}
		assertEquals(0, count);
		assertEquals(0, map.keySet().size());
	}

	private static class SlowProducer implements Iterator<String> {

		private final int delayTime;
		private final List<String> elements;

		public SlowProducer(int delayTime, String... elements) {
			this.delayTime = delayTime;
			this.elements = new ArrayList<>(Arrays.asList(elements));
		}

		@Override
		public boolean hasNext() {
			try {
				Thread.sleep(delayTime);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return !elements.isEmpty();
		}

		@Override
		public String next() {
			return elements.remove(0);
		}

	}

	private static class ErrorThrowingProducer implements Iterator<String> {

		private final int numElementsBeforeException;
		private int count = 0;

		public ErrorThrowingProducer(int numElementsBeforeException) {
			this.numElementsBeforeException = numElementsBeforeException;
		}

		@Override
		public boolean hasNext() {
			if (count++ > numElementsBeforeException) {
				throw new TestException("simulated failure!");
			}
			return true;
		}

		@Override
		public String next() {
			return "TEST";
		}

	}

	private static class TestException extends RuntimeException {

		public TestException(String message) {
			super(message);
		}

	}

}
