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
package com.avanza.gs.mongo;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.avanza.gs.mongo.util.NamedThreadFactory;
import com.avanza.gs.mongo.util.Require;
import com.gigaspaces.datasource.DataIterator;

/**
 * Used for merging multiple iterators into one iterator with a common bounded queue. The backed iterators are read in
 * parallel using a thread pool.
 * <p>
 * The developer who can find a good name for this class will be awarded with an ice cream
 *
 * @author Andreas Skoog
 *
 * @param <T>
 *            Type of data to iterate over
 */
public class ParallelIteratorIterator<T> implements DataIterator<T> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ExecutorService pool = Executors.newCachedThreadPool(new NamedThreadFactory("parallel-iterator"));
	private final ArrayBlockingQueue<T> queue = new ArrayBlockingQueue<>(10000);
	private CountDownLatch latch;

	private volatile RuntimeException error;
	private int numElementsLoaded = 0;

	/**
	 * @param iterators
	 *            >= 1
	 */
	@SafeVarargs
	public ParallelIteratorIterator(Iterator<T>... iterators) {
		this(Arrays.asList(iterators));
	}

	public ParallelIteratorIterator(Collection<Iterator<T>> iterators) {
		Require.notNull(iterators);
		latch = new CountDownLatch(iterators.size());
		iterators.stream().forEach(iter -> pool.submit(() -> drain(iter)));
	}

	private void drain(Iterator<T> iter) {
		log.info("Begin draining iterator");
		try {
			int numElements = 0;
			while (iter.hasNext()) {
				queue.put(iter.next());
				numElements++;
			}
			log.info("Finished draining iterator. Num elements forwarded: {}", numElements);
		} catch (RuntimeException e) {
			error = e;
			log.error("Error occurred when draining iterator", e);
			pool.shutdownNow();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			latch.countDown();
			if (latch.getCount() == 0) {
				pool.shutdown();
			}
		}
	}

	public static <T> Iterator<T> create(Collection<Iterator<T>> iterators) {
		return new ParallelIteratorIterator<>(iterators);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		// Extra check to handle initial result 
		while(latch.getCount() > 0 && queue.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		if(error != null) {
			throw error;
		}
		boolean hasNext = latch.getCount() > 0 || queue.size() > 0;
		if (!hasNext) {
			log.info("Finished loading space objects. Total number of elements loaded: {}", numElementsLoaded);
		}
		return hasNext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T next() {
		try {
			return queue.poll(5, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} finally {
			numElementsLoaded++;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() {
		/* not implemented */ }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		// Just to be safe, not acutally needed
		pool.shutdown();
	}

}
