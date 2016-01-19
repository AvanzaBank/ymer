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
package example.mirror;

import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.MirroredDocumentDefinitions;
import com.avanza.ymer.MongoConverterFactory;

import example.domain.SpaceFruit;


public class ExampleMirroredDocumentDefinitions implements MirroredDocumentDefinitions, MongoConverterFactory {
	
	private MongoDbFactory mongoDbFactory;

	@Autowired
	public ExampleMirroredDocumentDefinitions(MongoDbFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	@Override
	public Stream<MirroredObjectDefinition<?>> getDefinitions() {
		return Stream.of(
			MirroredObjectDefinition.create(SpaceFruit.class)
		);
	}

	@Override
	public MongoConverter createMongoConverter() {
		DbRefResolver dbRef = new DefaultDbRefResolver(mongoDbFactory);
		return new MappingMongoConverter(dbRef , new MongoMappingContext());
	}

}
