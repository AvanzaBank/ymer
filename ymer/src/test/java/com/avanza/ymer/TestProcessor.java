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

import java.util.Optional;

import org.bson.Document;

import com.avanza.ymer.plugin.Plugin;
import com.avanza.ymer.plugin.PostReadProcessor;
import com.avanza.ymer.plugin.PreWriteProcessor;

public class TestProcessor implements PreWriteProcessor, PostReadProcessor {

	public static class TestPlugin implements Plugin {

		@Override
		public Optional<PostReadProcessor> createPostReadProcessor(Class<?> spaceClass) {
			return Optional.of(new TestProcessor());
		}

		@Override
		public Optional<PreWriteProcessor> createPreWriteProcessor(Class<?> spaceClass) {
			return Optional.of(new TestProcessor());
		}

	}

	@Override
	public Document postRead(Document postRead) {
		if (postRead.containsKey("name")) {
			postRead.put("name", postRead.get("name").toString().substring(1));
		}
		return postRead;
	}

	@Override
	public Document preWrite(Document preWrite) {
		if (preWrite.containsKey("name")) {
			preWrite.put("name", "a" + preWrite.get("name"));
		}
		return preWrite;
	}

}
