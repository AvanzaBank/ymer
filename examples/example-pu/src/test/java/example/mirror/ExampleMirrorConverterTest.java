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

import java.util.Collection;
import java.util.List;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.test.YmerConverterTestBase;

import example.domain.SpaceFruit;

class ExampleMirrorConverterTest extends YmerConverterTestBase {

	@Override
	protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Override
	protected MongoConverter createMongoConverter(MongoDatabaseFactory mongoDbFactory) {
		return ExampleMirrorFactory.createMongoConverter(mongoDbFactory);
	}

	@Override
	protected Collection<ConverterTest<?>> testCases() {
		return List.of(
			new ConverterTest<>(new SpaceFruit("Apple", "France", true))
		);
	}
	
}
