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
import com.mongodb.DB;
/**
 * 
 * @author Elias Lindholm (elilin)
 *
 */
public class MirrorEnvironment {
	
	public static final String TEST_MIRROR_DB_NAME = "mirror_test_db";
	
	private Fongo mongoServer = new Fongo(MirrorEnvironment.class.getSimpleName());
	
	public MongoTemplate getMongoTemplate() {
		SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(mongoServer.getMongo(), TEST_MIRROR_DB_NAME);
		return new MongoTemplate(simpleMongoDbFactory);
	}
	
	private DB getMongoDb() {
		return this.mongoServer.getMongo().getDB(TEST_MIRROR_DB_NAME);
	}
	
	public void dropAllMongoCollections() {
		getMongoDb().getCollectionNames().forEach(this::dropColletion);
	}

	private void dropColletion(String collectionName) {
		getMongoDb().getCollection(collectionName).drop();
	}

	public String getDatabaseName() {
		return TEST_MIRROR_DB_NAME;
	}
	
	public ApplicationContext getMongoClientContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoClient", mongoServer.getMongo());
		context.refresh();
		return context;
	}

}
