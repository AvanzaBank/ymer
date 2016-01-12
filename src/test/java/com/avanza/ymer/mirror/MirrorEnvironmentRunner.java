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
package com.avanza.ymer.mirror;

import java.util.Properties;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.Mongo;
/**
 * 
 * @author Elias Lindholm (elilin)
 *
 */
public class MirrorEnvironmentRunner implements TestRule {
	
	public static final String TEST_MIRROR_DB_NAME = "mirror_test_db";
	
	private Fongo mongoServer = new Fongo(MirrorEnvironmentRunner.class.getSimpleName());
//	private EmbeddedJndiServer jndiServer = new EmbeddedJndiServer(FreePortScanner.getFreePort());
	private MirroredDocuments mirroredDocuments;

	private Properties props;
	
	public MirrorEnvironmentRunner(MirroredDocuments mirroredDocuments) {
		this.mirroredDocuments = mirroredDocuments;
	}
	
	public void start() throws Exception {
		// TODO: Remove start method of move creation of Fongo instance here
		
//		mongoServer.startMongoInstance();
		
//		ServerAddress mongoAddress = mongoServer.getMongo().getAddress();
//		props = new Properties();
//		props.setProperty("mongouri", "mongodb://" + mongoAddress.getHost() + ":" + mongoAddress.getPort());
//		props.setProperty("databasename", TEST_MIRROR_DB_NAME);
//		props.setProperty("numBackups", "1");
//		props.setProperty("numPrimaries", "1");
		
//		jndiServer.addPropertiesEntry("testEnvProps", props);
//		jndiServer.start();
//		System.setProperty("avanza.jndi.primary", "localhost:" + jndiServer.getPort());
	}
	
	public Properties getMongoProperties() {
		return props;
	}
	
	public MongoTemplate getMongoTemplate() {
		SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(mongoServer.getMongo(), TEST_MIRROR_DB_NAME);
		return new MongoTemplate(simpleMongoDbFactory);
	}
	
	public Mongo getMongo() {
		return this.mongoServer.getMongo();
	}
	
	public DB getMongoDb() {
		return this.mongoServer.getMongo().getDB(TEST_MIRROR_DB_NAME);
	}
	
	public void stop() throws Exception {
		// TODO: remove
//		mongoServer.stopMongoInstance();
//		jndiServer.stop();
	}

	@Override
	public Statement apply(final Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				try {
					start();
					base.evaluate();
				} finally {
					stop();
				}
			}
		};
	}

	public void dropAllMongoCollections() {
		for (MirroredDocument<?> mirroredDocument : mirroredDocuments.getMirroredDocuments()) {
			getMongoDb().getCollection(mirroredDocument.getCollectionName()).drop();
		}
	}

	public String getDatabaseName() {
		return TEST_MIRROR_DB_NAME;
	}
	
	public ApplicationContext getMongoClientContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongo", mongoServer.getMongo());
		context.refresh();
		return context;
	}

}
