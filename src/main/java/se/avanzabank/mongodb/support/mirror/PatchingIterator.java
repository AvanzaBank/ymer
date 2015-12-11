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
package se.avanzabank.mongodb.support.mirror;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import se.avanzabank.mongodb.util.NamedThreadFactory;

/**
 * Implementation note: Multithreaded patching to increase throughput.
 * 
 * @author Joakim Sahlström, Kristoffer Erlandsson, Andreas Skoog
 */
public class PatchingIterator<T> implements Iterator<T> {
	
	private static final int NUM_THREADS = 15;
	private static final int QUEUE_SIZE = 10000;

	private final Logger log = LoggerFactory.getLogger(getClass()); 
	
	private final Iterator<DBObject> source;
	private final ExecutorService executor;
	private final BlockingQueue<T> convertedObjectsQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
	// Will be set by worker threads if an exception occurs
	private volatile Exception occuredError = null;
	private DocumentPatcher<T> documentPatcher;
	private final AtomicLong numConverted = new AtomicLong();
	private final AtomicLong numRead = new AtomicLong();
	private final AtomicBoolean started = new AtomicBoolean(false);
	
	public PatchingIterator(Iterator<DBObject> source, MirroredDocument<T> document, DocumentConverter documentConverter, SpaceObjectFilter<T> spaceObjectFilter) {
		this(source, new DocumentPatcherImpl<>(document, documentConverter, spaceObjectFilter));
	}
	
	public PatchingIterator(Iterator<DBObject> source, DocumentPatcher<T> patcher) {
		this.documentPatcher = Objects.requireNonNull(patcher);
		this.source = Objects.requireNonNull(source);
		executor = new ThreadPoolExecutor(NUM_THREADS, NUM_THREADS, 1, TimeUnit.MINUTES,
				new ArrayBlockingQueue<Runnable>(QUEUE_SIZE),
				new NamedThreadFactory("PatchingIteratorWorker-" + documentPatcher.getCollectionName()),
				new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	private void readFromSourceAndStartConvertTasks() {
		try {
			while (source.hasNext()) {
				DBObject nextObject = source.next();
				executor.execute(() -> convertAndEnqueueResult(nextObject));
			}
			executor.shutdown();
		} catch (Exception e) {
			occuredError = e;
			log.error("Failed convert collection " + documentPatcher.getCollectionName(), e);
			executor.shutdownNow();
		}
	}

	private void convertAndEnqueueResult(DBObject dbObject) {
		try {
			tryConvertAndEnqueue(dbObject);
		} catch (Exception e) {
			log.error("Failed to convert and enqueue object for collection " + documentPatcher.getCollectionName() + ". Object to convert was " + dbObject, e);
			occuredError = e;
			executor.shutdownNow();
		}
	}

	private void tryConvertAndEnqueue(DBObject dbObject) {
		Optional<T> converted = documentPatcher.patchAndConvert(new BasicDBObject(dbObject.toMap()));
		converted.ifPresent(this::enqueueConvertedObject);
	}
	
	private void enqueueConvertedObject(T converted) {
		try {
			if (!convertedObjectsQueue.offer(converted, 5, TimeUnit.MINUTES)) {
				throw new RuntimeException("Failed to offer to queue for collection "
						+ documentPatcher.getCollectionName() + ", object: " + converted);
			}
			long num = numConverted.incrementAndGet();
			if (num % 10000 == 0) {
				log.info("Status: {} entries converted for collection {}", num, documentPatcher.getCollectionName());
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while inserting to queue, object: " + converted, e);
		}
	}
	
	@Override
	public boolean hasNext() {
		if (!started.get()) {
			executor.execute(this::readFromSourceAndStartConvertTasks);
			started.set(true);
		}

		long start = System.nanoTime();
		while (!allObjectsConverted()) {
			if (!convertedObjectsQueue.isEmpty()) {
				return true;
			}
			sleep(10);
			if (System.nanoTime() - start > TimeUnit.MINUTES.toNanos(5)) {				
				throw new ConversionFailedException("Timeout when deciding if more objects are available for collection " + documentPatcher.getCollectionName());
			}
		}
		throwErrorIfOcurredInWorkerThreads();
		return !convertedObjectsQueue.isEmpty();
	}

	private void sleep(int sleepTimeMillis) {
		try {
			Thread.sleep(sleepTimeMillis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private boolean allObjectsConverted() {
		return executor.isTerminated();
	}
	

	@Override
	public T next() {
		if (!hasNext()) {
			throw new NoSuchElementException("No more elements for collection " + documentPatcher.getCollectionName());
		}
		throwErrorIfOcurredInWorkerThreads();
		long read = numRead.incrementAndGet();
		if (read % 10000 == 0) {
			log.info("Status: {} entries read for collection {}", read, documentPatcher.getCollectionName());
		}
		return convertedObjectsQueue.poll();
	}

	private void throwErrorIfOcurredInWorkerThreads() {
		if (occuredError != null) {
			throw new ConversionFailedException("Error on a worker thread, will not continue", occuredError);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported");
	}
	
	@SuppressWarnings("serial")
	public static class ConversionFailedException extends RuntimeException {

		public ConversionFailedException(String message, Throwable cause) {
			super(message, cause);
		}

		public ConversionFailedException(String message) {
			super(message);
		}
		
	}

}
