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
package se.avanzabank.mongodb.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.gigaspaces.datasource.DataIterator;

import se.avanzabank.mongodb.util.Require;

/**
 * Used for merging multiple iterators into one iterator, cleverly stolen from SO by
 *
 * @author joasah Joakim Sahlström
 *
 * @param <T> Type of data iterated over
 */
public class IteratorIterator<T> implements DataIterator<T> {
	private final List<Iterator<T>> iterators;
	private int current;

	/**
	 * @param iterators >= 1
	 */
	@SafeVarargs
	public IteratorIterator(Iterator<T>... iterators) {
		this(Arrays.asList(iterators));
	}

	public IteratorIterator(Collection<Iterator<T>> iterators) {
		Require.notNull(iterators);
    	this.iterators = new ArrayList<>(iterators);
    	current = 0;
	}

	public static <T> Iterator<T> create(Collection<Iterator<T>> iterators) {
		return new IteratorIterator<>(iterators);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {
    	updateToCurrentIterator();
    	return current < iterators.size();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T next() {
		updateToCurrentIterator();
		return iterators.get(current).next();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() { /* not implemented */ }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		// do nothing
	}

	private void updateToCurrentIterator() {
		while (current < iterators.size() && !iterators.get(current).hasNext()) {
			current++;
		}
	}

}
