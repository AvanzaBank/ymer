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

import org.bson.Document;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MirrorEnvironment extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(MirrorEnvironment.class);
	private static final String DEFAULT_MONGOVERSION = "4.0";

	public static final String TEST_MIRROR_DB_NAME = "mirror_test_db";
	private static final MongoDBContainer mongoDBContainer = setupMongoDb();
	private final MongoClient mongoClient;

	private static MongoDBContainer setupMongoDb() {
		String mongoVersionProperty = System.getProperty("integrationTest.mongoVersion");
		String mongoVersion;
		if (mongoVersionProperty != null) {
			LOG.info("Using supplied mongoVersion {} for integration tests", mongoVersionProperty);
			mongoVersion = mongoVersionProperty;
		} else {
			LOG.info("Using default mongoVersion {} for integration tests", DEFAULT_MONGOVERSION);
			mongoVersion = DEFAULT_MONGOVERSION;
		}
		return new MongoDBContainer(DockerImageName.parse("mongo").withTag(mongoVersion));
	}

	public MirrorEnvironment() {
		if (!mongoDBContainer.isRunning()) {
			mongoDBContainer.start();
			// leave container running until JVM stops
			Runtime.getRuntime().addShutdownHook(new Thread(mongoDBContainer::stop));
		}
		mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl());
	}

	public MongoTemplate getMongoTemplate() {
		MongoDatabaseFactory simpleMongoDbFactory = new SimpleMongoClientDatabaseFactory(mongoClient, TEST_MIRROR_DB_NAME);
		return new MongoTemplate(simpleMongoDbFactory);
	}

	private MongoDatabase getMongoDatabase() {
		return this.mongoClient.getDatabase(TEST_MIRROR_DB_NAME);
	}

	public void reset() {
		getMongoDatabase().drop();
	}

	public ApplicationContext getMongoClientContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoDbFactory", new SimpleMongoClientDatabaseFactory(mongoClient, TEST_MIRROR_DB_NAME));
		context.refresh();
		return context;
	}

	public void removeFormatVersion(Class<?> dataType, Object id) {
		String collectionName = dataType.getSimpleName().substring(0, 1).toLowerCase() + dataType.getSimpleName().substring(1);
		MongoCollection<Document> collection = getMongoDatabase().getCollection(collectionName);
		collection.findOneAndUpdate(eq("_id", id), unset("_formatVersion"));
	}

}
