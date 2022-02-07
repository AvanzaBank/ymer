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
/*
spaceSyncEndpoint * Copyright 2015 Avanza Bank AB
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
package example.tests;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import example.domain.SpaceCar;
import example.domain.SpaceFruit;

public class InitialLoadTest {

	@Rule
	public final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:3.6");

	private MongoClient mongoClient;

	private RunningPu pu;

	@Before
	public void setUp() {
		mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl());
	}

	@After
	public void shutdownPus() throws Exception {
		closeSafe(pu);
		mongoClient.close();
	}

	private MongoDatabase getDatabase(String databaseName) {
		return mongoClient.getDatabase(databaseName);
	}
	
	@Test
	public void intialLoadDemo() throws Exception {
		Document banana = new Document(Map.of("_id", "banana", "origin", "Brazil"));
		getDatabase("exampleDb").getCollection("spaceFruit").insertOne(banana);
		Document apple = new Document(Map.of("_id", "apple", "origin", "France"));
		getDatabase("exampleDb").getCollection("spaceFruit").insertOne(apple);
		getDatabase("exampleDb").getCollection("spaceCar").insertOne(new Document("name", "Volvo")); // SpaceCar is not Mirrored
		
		
		pu = PuConfigurers.partitionedPu("classpath:example-pu.xml")
								    .parentContext(createSingleInstanceAppContext(mongoClient))
								    .configure();
		
		pu.start();
		
		
		GigaSpace gigaSpace = pu.getClusteredGigaSpace();
		
		assertEquals(0, gigaSpace.count(new SpaceCar()));
		assertEquals(2, gigaSpace.count(new SpaceFruit()));
	}

	private ApplicationContext createSingleInstanceAppContext(MongoClient mongo) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoClient", new SimpleMongoClientDatabaseFactory(mongo, "exampleDb"));
		context.refresh();
		return context;
	}
	
	private void closeSafe(AutoCloseable a) {
		try {
			a.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
