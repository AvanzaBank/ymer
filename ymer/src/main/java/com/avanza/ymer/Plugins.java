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

import com.avanza.ymer.plugin.Plugin;
import com.avanza.ymer.plugin.PostReadProcessor;
import com.avanza.ymer.plugin.PreWriteProcessor;
import com.mongodb.DBObject;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Plugins {
	private final Set<Plugin> plugins;
	private final Map<Class<?>, PostReadProcessor> postReadProcessors = new HashMap<>();
	private final Map<Class<?>, PreWriteProcessor> preWriteProcessors = new HashMap<>();

	public static Plugins empty() {
		return new Plugins(Collections.emptySet());
	}

	public Plugins(Set<Plugin> plugins) {
		this.plugins = Objects.requireNonNull(plugins);
	}

	public PostReadProcessor getPostReadProcessing(Class<?> dataType) {
		return postReadProcessors.computeIfAbsent(dataType, dt ->
			new PostReadProcessor() {
				private final Set<PostReadProcessor> postReadProcessors = plugins.stream()
						.map(p -> p.createPostReadProcessor(dt))
						.flatMap(OptionalHelper::toStream)
						.collect(Collectors.toSet());
				@Override
				public DBObject postRead(DBObject postRead) {
					for (PostReadProcessor processor : postReadProcessors) {
						postRead = processor.postRead(postRead);
					}
					return postRead;
				}
			});
	}

	public PreWriteProcessor getPreWriteProcessing(Class<?> dataType) {
		return preWriteProcessors.computeIfAbsent(dataType, dt ->
			new PreWriteProcessor() {
				private final Set<PreWriteProcessor> preWriteProcessors = plugins.stream()
						.map(p -> p.createPreWriteProcessor(dataType))
						.flatMap(OptionalHelper::toStream)
						.collect(Collectors.toSet());

				@Override
				public DBObject preWrite(DBObject preWrite) {
					for (PreWriteProcessor processor : preWriteProcessors) {
						preWrite = processor.preWrite(preWrite);
					}
					return preWrite;
				}
			});
	}

	private static class OptionalHelper {
		private static <T> Stream<T> toStream(Optional<T> o) {
			return o.map(Stream::of).orElse(Stream.empty());
		}
	}

}
