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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.MongoDatabaseFactory;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.MirroredObjectsConfiguration;
import com.avanza.ymer.YmerFactory;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import example.domain.SpaceFruit;

public class ExampleMirrorFactory {
	
	private final MongoDatabaseFactory mongoDbFactory;
	private final MirroredObjectsConfiguration mirroredObjectsConfiguration = new ExampleMirroredObjectsConfiguration();

	@Autowired
	public ExampleMirrorFactory(MongoDatabaseFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	public SpaceDataSource createSpaceDataSource() {
		return new YmerFactory(mongoDbFactory, mirroredObjectsConfiguration).createSpaceDataSource();
	}
	
	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		return new YmerFactory(mongoDbFactory, mirroredObjectsConfiguration).createSpaceSynchronizationEndpoint();
	}

	@WritingConverter
	static class FruitToBson implements Converter<SpaceFruit, DBObject> {
		@Override
		public DBObject convert(SpaceFruit fruit) {
			BasicDBObject result = new BasicDBObject();
			result.put("_id", fruit.getName());
			result.put("origin", fruit.getOrigin());
			result.put("organic", fruit.isOrganic());
			return result;
		}
	}

	@ReadingConverter
	static class BsonToFruit implements Converter<DBObject, SpaceFruit> {

		@Override
		public SpaceFruit convert(DBObject dbObject) {
			return new SpaceFruit((String) dbObject.get("_id"), (String) dbObject.get("origin"), (boolean) dbObject.get("organic"));
		}
	}

	static class ExampleMirroredObjectsConfiguration implements MirroredObjectsConfiguration {
		@Override
		public Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
			return getDefinitions();
		}

		@Override
		public List<Converter<?, ?>> getCustomConverters() {
			return ExampleMirrorFactory.getCustomConverters();
		}
	}

	static Collection<MirroredObjectDefinition<?>> getDefinitions() {
		return Arrays.asList(
			MirroredObjectDefinition.create(SpaceFruit.class)
									.documentPatches(new SpaceFruitV1ToV2Patch())
		);
	}

	static List<Converter<?, ?>> getCustomConverters() {
		List<Converter<?, ?>> converters = new ArrayList<>();
		converters.add(new FruitToBson());
		converters.add(new BsonToFruit());
		return converters;
	}
}
