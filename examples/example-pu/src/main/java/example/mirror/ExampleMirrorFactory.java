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
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.YmerFactory;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import example.domain.SpaceFruit;

public class ExampleMirrorFactory {
	
	private final MongoDatabaseFactory mongoDbFactory;
	
	@Autowired
	public ExampleMirrorFactory(MongoDatabaseFactory mongoDbFactory) {
		this.mongoDbFactory = mongoDbFactory;
	}

	public SpaceDataSource createSpaceDataSource() {
		return new YmerFactory(mongoDbFactory, createMongoConverter(mongoDbFactory), getDefinitions()).createSpaceDataSource();
	}
	
	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		return new YmerFactory(mongoDbFactory, createMongoConverter(mongoDbFactory), getDefinitions()).createSpaceSynchronizationEndpoint();
	}

	static MongoConverter createMongoConverter(MongoDatabaseFactory mongoDbFactory) {
		DbRefResolver dbRef = new DefaultDbRefResolver(mongoDbFactory);
		MappingMongoConverter converter = new MappingMongoConverter(dbRef , new MongoMappingContext());
		List<Converter<?, ?>> converters = new ArrayList<>();
		converters.add(new FruitToBson());
		converters.add(new BsonToFruit());
		converter.setCustomConversions(new MongoCustomConversions(converters));
		converter.afterPropertiesSet();
		return converter;
	}
	
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
	
	static class BsonToFruit implements Converter<DBObject, SpaceFruit> {

		@Override
		public SpaceFruit convert(DBObject dbObject) {
			return new SpaceFruit((String) dbObject.get("_id"), (String) dbObject.get("origin"), (boolean) dbObject.get("organic"));
		}
	}
	
	static Collection<MirroredObjectDefinition<?>> getDefinitions() {
		return Arrays.asList(
			MirroredObjectDefinition.create(SpaceFruit.class)
									.documentPatches(new SpaceFruitV1ToV2Patch())
		);
	}
	
}
