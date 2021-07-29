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

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.unset;

import java.util.function.Consumer;

import org.bson.Document;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.testcontainers.containers.MongoDBContainer;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MirrorEnvironment implements AutoCloseable{

	public static final String TEST_MIRROR_DB_NAME = "mirror_test_db";
	private final MongoDBContainer mongoDBContainer;
	private final MongoClient mongoClient;

	public MirrorEnvironment() {
		mongoDBContainer = new MongoDBContainer("mongo:3.6");
		mongoDBContainer.start();

		mongoClient = new MongoClient(new MongoClientURI(mongoDBContainer.getReplicaSetUrl()));
	}

	@Override
	public void close() {
		mongoDBContainer.stop();
	}

	public MongoTemplate getMongoTemplate() {
		SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(mongoClient, TEST_MIRROR_DB_NAME);
		return new MongoTemplate(simpleMongoDbFactory);
	}

	private MongoDatabase getMongoDatabase() {
		return this.mongoClient.getDatabase(TEST_MIRROR_DB_NAME);
	}

	public void dropAllMongoCollections() {
		getMongoDatabase().listCollectionNames()
						  .forEach((Consumer<? super String>) this::dropCollection);
	}

	private void dropCollection(String collectionName) {
		getMongoDatabase().getCollection(collectionName).drop();
	}

	public ApplicationContext getMongoClientContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoDbFactory", new SimpleMongoDbFactory(mongoClient, TEST_MIRROR_DB_NAME));
		context.refresh();
		return context;
	}

	public void removeFormatVersion(Class<?> dataType, Object id) {
		String collectionName = dataType.getSimpleName().substring(0, 1).toLowerCase() + dataType.getSimpleName().substring(1);
		MongoCollection<Document> collection = getMongoDatabase().getCollection(collectionName);
		collection.findOneAndUpdate(eq("_id", id), unset("_formatVersion"));
	}

}
