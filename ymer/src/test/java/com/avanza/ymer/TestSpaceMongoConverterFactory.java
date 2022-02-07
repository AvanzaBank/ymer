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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

public class TestSpaceMongoConverterFactory {

	private final MongoDatabaseFactory mongoDbFactory;

	@Autowired
	public TestSpaceMongoConverterFactory(MongoDatabaseFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	public MongoConverter createMongoConverter() {
		return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
	}

}
