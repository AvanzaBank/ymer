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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.avanza.ymer.plugin.Plugin;
import com.avanza.ymer.plugin.PostReadProcessor;
import com.avanza.ymer.plugin.PreWriteProcessor;
import com.mongodb.BasicDBObjectBuilder;

public class PluginsTest {

	private static class MyPlugin implements Plugin {
		private final String postReadAppend;
		private final String preWriteAppend;
		public MyPlugin(String postReadAppend, String preWriteAppend) {
			this.postReadAppend = postReadAppend;
			this.preWriteAppend = preWriteAppend;
		}

		@Override
		public Optional<PostReadProcessor> createPostReadProcessor(Class<?> spaceClass) {
			return Optional.of((dbo) -> {
				dbo.put("name", dbo.get("name") + postReadAppend);
				return dbo;
			});
		}

		@Override
		public Optional<PreWriteProcessor> createPreWriteProcessor(Class<?> spaceClass) {
			return Optional.of((dbo) -> {
				dbo.put("name", dbo.get("name") + preWriteAppend);
				return dbo;
			});
		}
	}

	@Test
	public void processorsAreRegistered() throws Exception {
		Plugins plugins = new Plugins(Collections.singleton(new MyPlugin("A", "B")));
		assertEquals("|A", plugins.getPostReadProcessing(Object.class).postRead(BasicDBObjectBuilder.start("name", "|").get()).get("name"));
		assertEquals("|B", plugins.getPreWriteProcessing(Object.class).preWrite(BasicDBObjectBuilder.start("name", "|").get()).get("name"));
	}

	@Test
	public void differentInstancesAreReturnedForDifferentClasses() throws Exception {
		Plugins plugins = new Plugins(Collections.singleton(new MyPlugin("A", "B")));
		assertNotSame(plugins.getPostReadProcessing(Object.class), plugins.getPostReadProcessing(String.class));
	}

	@Test
	public void sameInstanceIsReturnedForSameClass() throws Exception {
		Plugins plugins = new Plugins(Collections.singleton(new MyPlugin("A", "B")));
		assertSame(plugins.getPostReadProcessing(Object.class), plugins.getPostReadProcessing(Object.class));
	}

	@Test
	public void allProcessorsAreRegistered() throws Exception {
		Plugins plugins = new Plugins(new HashSet<>(Arrays.asList(new MyPlugin("A", "B"), new MyPlugin("C", "D"))));
		assertTrue(plugins.getPostReadProcessing(Object.class).postRead(BasicDBObjectBuilder.start("name", "|").get()).get("name").toString().contains("A"));
		assertTrue(plugins.getPostReadProcessing(Object.class).postRead(BasicDBObjectBuilder.start("name", "|").get()).get("name").toString().contains("C"));
		assertTrue(plugins.getPreWriteProcessing(Object.class).preWrite(BasicDBObjectBuilder.start("name", "|").get()).get("name").toString().contains("B"));
		assertTrue(plugins.getPreWriteProcessing(Object.class).preWrite(BasicDBObjectBuilder.start("name", "|").get()).get("name").toString().contains("D"));
	}

	@Test
	public void shouldPreventConcurrentModificationException() throws Exception {
		// Arrange
		final Plugin slowPlugin = mock(Plugin.class);
		doAnswer(i -> {
			Thread.sleep(10);
			return Optional.empty();
		}).when(slowPlugin).createPostReadProcessor(any());
		final AtomicReference<Exception> thrownException = new AtomicReference<>();

		for (int i = 0; i < 100; ++i) {
			final Plugins plugins = new Plugins(Collections.singleton(slowPlugin));
			final Runnable concurrentModificationOfMap = () -> {
				try {
					plugins.getPreWriteProcessing(this.getClass());
				} catch (Exception e) {
					e.printStackTrace();
					thrownException.set(e);
				}
			};

			// Act
			final Thread t1 = new Thread(concurrentModificationOfMap);
			final Thread t2 = new Thread(concurrentModificationOfMap);
			t1.start();
			t2.start();

			// Assert
			t1.join();
			t2.join();
			assertNull(thrownException.get());
		}
	}

}
