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
package com.avanza.ymer.plugin;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.DBObject;

public class Plugins {
	private final Set<PostReadProcessor> postReadProcessors = new HashSet<>();
	private final Set<PreWriteProcessor> preWriteProcessors = new HashSet<>();

	public static Plugins empty() {
		return new Plugins(Collections.emptySet());
	}

	public Plugins(Set<Object> plugins) {
		plugins.stream()
			.filter(p -> (p instanceof PostReadProcessor))
			.map(PostReadProcessor.class::cast)
			.forEach(postReadProcessors::add);

		plugins.stream()
			.filter(p -> (p instanceof PreWriteProcessor))
			.map(PreWriteProcessor.class::cast)
			.forEach(preWriteProcessors::add);
	}

	public PostReadProcessor getPostReadProcessing() {
		return new PostReadProcessor() {
			@Override
			public DBObject postRead(DBObject postRead, Class<?> dataType) {
				for (PostReadProcessor processor : postReadProcessors) {
					postRead = processor.postRead(postRead, dataType);
				}
				return postRead;
			}
		};
	}

	public PreWriteProcessor getPreWriteProcessing() {
		return new PreWriteProcessor() {
			@Override
			public DBObject preWrite(DBObject preWrite, Class<?> dataType) {
				for (PreWriteProcessor processor : preWriteProcessors) {
					preWrite = processor.preWrite(preWrite, dataType);
				}
				return preWrite;
			}
		};
	}

}
