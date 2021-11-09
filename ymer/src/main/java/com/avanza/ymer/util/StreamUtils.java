package com.avanza.ymer.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class StreamUtils {
	private StreamUtils() {
	}

	/**
	 * Returns a stream with connected, non-overlapping buffers from the {@code source} stream.
	 * The buffers will be of size {@code bufferSize} maximum, where the last one might be smaller.
	 * The {@code source} stream will only be consumed according to the resulting stream,
	 * i.e. if the resulting stream is limited to 2 elements, only at most {@code 2 *  bufferSize} elements will be consumed
	 * from the {@code source} stream.
	 * <p>
	 * Note: if the resulting stream is consumed (a terminal operation is used), the {@code source} stream will also be consumed.
	 * If the resulting stream is not consumed, the {@code source} stream will not be either.
	 */
	public static <T> Stream<List<T>> buffer(Stream<T> source, int bufferSize) {
		return StreamSupport.stream(() -> {
			Spliterator<T> spliterator = source.spliterator();
			return new AbstractSpliterator<List<T>>(Long.MAX_VALUE, spliterator.characteristics()) {
				@Override
				public boolean tryAdvance(Consumer<? super List<T>> action) {
					List<T> batch = new ArrayList<>(bufferSize);

					for (int i = 0; i < bufferSize; i++) {
						if (!spliterator.tryAdvance(batch::add)) {
							break;
						}
					}
					if (batch.isEmpty()) {
						return false;
					} else {
						action.accept(batch);
						return true;
					}
				}
			};
		}, 0, false);
	}
}
