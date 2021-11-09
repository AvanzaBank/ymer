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
	 * The {@code source} stream will only be consumed according to the buffering stream,
	 * i.e. if the buffering stream is limited to 2 elements, only at most {@code 2 *  bufferSize} elements will be consumed
	 * from the {@code source} stream.
	 * <p>
	 * Note: if the buffering stream is consumed (a terminal operation is used), the {@code source} stream will also be consumed.
	 * If the buffering stream is not consumed, the {@code source} stream will not be either.
	 */
	public static <T> Stream<List<T>> buffer(Stream<T> source, int bufferSize) {
		return StreamSupport.stream(() -> {
			Spliterator<T> spliterator = source.spliterator();
			long estimatedSize = spliterator.estimateSize();
			if (estimatedSize != Long.MAX_VALUE) {
				estimatedSize = (int) Math.ceil((double) estimatedSize / (double) bufferSize);
			}
			return new AbstractSpliterator<List<T>>(estimatedSize, 0) {
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
		}, 0, false).onClose(source::close);
	}

}
