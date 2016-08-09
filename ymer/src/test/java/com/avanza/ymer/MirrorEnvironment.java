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

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MirrorEnvironment {

	public static final String TEST_MIRROR_DB_NAME = "mirror_test_db";

	private final Fongo mongoServer = new Fongo(MirrorEnvironment.class.getSimpleName());

	public MongoTemplate getMongoTemplate() {
		SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(mongoServer.getMongo(), TEST_MIRROR_DB_NAME);
		return new MongoTemplate(simpleMongoDbFactory);
	}

	private DB getMongoDb() {
		return this.mongoServer.getMongo().getDB(TEST_MIRROR_DB_NAME);
	}

	public void dropAllMongoCollections() {
		getMongoDb().getCollectionNames().forEach(this::dropCollection);
	}

	private void dropCollection(String collectionName) {
		getMongoDb().getCollection(collectionName).drop();
	}

	public ApplicationContext getMongoClientContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoDbFactory", new SimpleMongoDbFactory(mongoServer.getMongo(), TEST_MIRROR_DB_NAME));
		context.refresh();
		return context;
	}

	public void removeFormatVersion(Class<?> dataType, Object id) {
		String collectionName = dataType.getSimpleName().substring(0, 1).toLowerCase() + dataType.getSimpleName().substring(1);
		DBCollection collection = getMongoDb().getCollection(collectionName);
		DBObject idQuery = BasicDBObjectBuilder.start("_id", id).get();
		DBCursor cursor = collection
			.find(idQuery);

		cursor.next();
		DBObject obj = cursor.curr();
		cursor.close();

		obj.removeField("_formatVersion");
		collection.update(idQuery, obj);
	}

}
