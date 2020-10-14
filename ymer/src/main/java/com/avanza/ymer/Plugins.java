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

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.avanza.ymer.plugin.Plugin;
import com.avanza.ymer.plugin.PostReadProcessor;
import com.avanza.ymer.plugin.PreWriteProcessor;
import com.mongodb.DBObject;

class Plugins {
	private final Set<Plugin> plugins;
	private final Map<Class<?>, PostReadProcessor> postReadProcessors = new HashMap<>();
	private final Map<Class<?>, PreWriteProcessor> preWriteProcessors = new HashMap<>();

	public static Plugins empty() {
		return new Plugins(emptySet());
	}

	public Plugins(Set<Plugin> plugins) {
		this.plugins = requireNonNull(plugins);
	}

	public PostReadProcessor getPostReadProcessing(Class<?> dataType) {

		PostReadProcessor processor = postReadProcessors.get(dataType);

		if(processor == null) {
			PostReadProcessor postReadProcessor = new PostReadProcessor() {
				private final Set<PostReadProcessor> postReadProcessors =
						plugins.stream()
							   .map(p -> p.createPostReadProcessor(dataType))
							   .flatMap(Optional::stream)
							   .collect(toSet());

				@Override
				public DBObject postRead(DBObject postRead) {
					for (PostReadProcessor processor : postReadProcessors) {
						postRead = processor.postRead(postRead);
					}
					return postRead;
				}
			};
			postReadProcessors.put(dataType, postReadProcessor);
			return postReadProcessor;
		}
		return processor;
	}

	public PreWriteProcessor getPreWriteProcessing(Class<?> dataType) {

		PreWriteProcessor processor = preWriteProcessors.get(dataType);
		if(processor == null) {
			PreWriteProcessor preWriteProcessor = new PreWriteProcessor() {
				private final Set<PreWriteProcessor> preWriteProcessors =
						plugins.stream()
							   .map(p -> p.createPreWriteProcessor(dataType))
							   .flatMap(Optional::stream)
							   .collect(toSet());

				@Override
				public DBObject preWrite(DBObject preWrite) {
					for (PreWriteProcessor processor : preWriteProcessors) {
						preWrite = processor.preWrite(preWrite);
					}
					return preWrite;
				}
			};
			preWriteProcessors.put(dataType, processor);
			return preWriteProcessor;
		}
		return processor;
	}
}
