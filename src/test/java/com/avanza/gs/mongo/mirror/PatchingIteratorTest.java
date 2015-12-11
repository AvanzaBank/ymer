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
package com.avanza.gs.mongo.mirror;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.Mockito;

import com.avanza.gs.mongo.mirror.PatchingIterator.ConversionFailedException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class PatchingIteratorTest {

	@Test
	public void oneObject() throws Exception {
		DocumentPatcher<String> patcher = new FakePatcher("foo");
		DBObject dbObject = Mockito.mock(DBObject.class);
		PatchingIterator<String> p = new PatchingIterator<String>(iteratorOf(dbObject), patcher);
		assertThat(toCollection(p), contains("foo"));
	}

	@Test
	public void manyObjects() throws Exception {
		DocumentPatcher<String> patcher = new FakePatcher("foo", "bar", "baz");
		DBObject dbObject = Mockito.mock(DBObject.class);
		PatchingIterator<String> p = new PatchingIterator<String>(iteratorOf(dbObject, dbObject, dbObject), patcher);
		assertThat(toCollection(p), containsInAnyOrder("foo", "bar", "baz"));
	}

	@Test
	public void missingIsSkipped() throws Exception {
		DocumentPatcher<String> patcher = new FakePatcher("foo", "bar", null);
		DBObject dbObject = Mockito.mock(DBObject.class);
		PatchingIterator<String> p = new PatchingIterator<String>(iteratorOf(dbObject, dbObject, dbObject), patcher);
		assertThat(toCollection(p), containsInAnyOrder("foo", "bar"));
	}
	
	@Test
	public void veryManyObjects() throws Exception {
		String[] patcherSequence = Collections.nCopies(10000, "foo").toArray(new String[]{});
		DocumentPatcher<String> patcher = new FakePatcher(patcherSequence);
		DBObject dbObject = Mockito.mock(DBObject.class);
		DBObject[] dbObjects = Collections.nCopies(10000, dbObject).toArray(new DBObject[]{});
		PatchingIterator<String> p = new PatchingIterator<>(iteratorOf(dbObjects), patcher);
		assertThat(toCollection(p), hasItems("foo", "foo"));
	}

	@Test(expected=ConversionFailedException.class)
	public void exceptionInConverterPropagates() throws Exception {
		DocumentPatcher<String> patcher = new FakePatcher("foo", "throwException", null);
		DBObject dbObject = Mockito.mock(DBObject.class);
		PatchingIterator<String> p = new PatchingIterator<String>(iteratorOf(dbObject, dbObject, dbObject), patcher);
		toCollection(p);
	}
	
	@Test(expected=ConversionFailedException.class)
	public void exceptionInSourceIteratorPropagates() throws Exception {
		DocumentPatcher<String> patcher = new FakePatcher("foo");
		toCollection(new PatchingIterator<String>(new ThrowingIterator(), patcher));
	}
	
	public <T> Iterator<T> iteratorOf(@SuppressWarnings("unchecked") T ... elements) {
		return Arrays.asList(elements).iterator();
	}
	
	public <T> Collection<T> toCollection(Iterator<T> iterator) {
		Collection<T> l = new ArrayList<>();
		iterator.forEachRemaining(l::add);
		return l;
	}
	
	private static class ThrowingIterator implements Iterator<DBObject> {
		
		private volatile boolean done = false;

		@Override
		public boolean hasNext() {
			return !done;
		}

		@Override
		public DBObject next() {
			done = true;
			throw new RuntimeException();
		}
		
	}

	private static class FakePatcher implements DocumentPatcher<String> {
		
		private final AtomicInteger pos = new AtomicInteger();
		private final String[] returnSequence;

		public FakePatcher(String ... returnSequence) {
			this.returnSequence = returnSequence;
		}
		
		@Override
		public Optional<String> patchAndConvert(BasicDBObject dbObject) {
			int i = pos.getAndIncrement();
			String s = returnSequence[i];
			if ("throwException".equals(s)) {
				throw new RuntimeException();
			}
			return Optional.ofNullable(s);
		}
		
		@Override
		public String getCollectionName() {
			return "test-collection";
		}

	}

}
