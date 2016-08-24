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

import java.util.Optional;

import com.mongodb.DBObject;

/**
 * {@link Plugin}s create processors for ymer<br/>
 * <br/>
 * A processor is required to be thread safe.<br/>
 * Multiple processors of each type MAY be requested by ymer, so if a processor is synchronized or otherwise would benefit from running in multiple instances it is recommended that a new instance is returned per spaceClass<br/>
 * <br/>
 * A processor may manipulate the {@link DBObject}s that are passed to it
 */
public interface Plugin {
	Optional<PostReadProcessor> createPostReadProcessor(Class<?> spaceClass);
	Optional<PreWriteProcessor> createPreWriteProcessor(Class<?> spaceClass);
}
