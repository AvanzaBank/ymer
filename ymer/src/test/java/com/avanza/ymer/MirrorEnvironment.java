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

import java.net.UnknownHostException;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MirrorEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(MirrorEnvironment.class);
	private static final String DEFAULT_MONGOVERSION = "4.0";

	private static final MongoDBContainer mongoDBContainer = setupMongoDb();
	public static final String TEST_MIRROR_DB_NAME = "mirror_test_db";

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
	}

	public MongoTemplate getMongoTemplate() {
		return new MongoTemplate(getMongoDbFactory());
	}

	private MongoDbFactory getMongoDbFactory() {
		try {
			MongoClientURI uri = new MongoClientURI(mongoDBContainer.getReplicaSetUrl(TEST_MIRROR_DB_NAME));
			return new SimpleMongoDbFactory(uri);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public DB getMongoDb() {
		return getMongoTemplate().getDb();
	}

	public void dropAllMongoCollections() {
		getMongoDb().getCollectionNames().forEach(this::dropCollection);
	}

	private void dropCollection(String collectionName) {
		getMongoDb().getCollection(collectionName).drop();
	}

	public ApplicationContext getMongoClientContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoDbFactory", getMongoDbFactory());
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
