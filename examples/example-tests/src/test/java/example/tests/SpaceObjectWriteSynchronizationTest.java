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

import java.util.stream.StreamSupport;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import example.domain.SpaceFruit;

public class SpaceObjectWriteSynchronizationTest {

	@Rule
	public final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:3.6");

	private final Logger log = LoggerFactory.getLogger(getClass());

	private RunningPu pu;
	private RunningPu mirrorPu;

	private MongoClient mongoClient;

	@Before
	public void setUp() {
		mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl());
	}

	@After
	public void shutdownPus() throws Exception {
		closeSafe(pu);
		closeSafe(mirrorPu);
		mongoClient.close();
	}
	
	@Test
	public void mirrorDemo() throws Exception {
		pu = PuConfigurers.partitionedPu("classpath:example-pu.xml")
									.numberOfPrimaries(1)
									.numberOfBackups(1)
								    .parentContext(createSingleInstanceAppContext(mongoClient))
								    .configure();
		pu.start();

		mirrorPu = PuConfigurers.mirrorPu("classpath:example-mirror-pu.xml")
		   	     				.parentContext(createSingleInstanceAppContext(mongoClient))
		   	     				.configure();
		mirrorPu.start();
		
		
		GigaSpace gigaSpace = pu.getClusteredGigaSpace();
		gigaSpace.write(new SpaceFruit("banana", "Brazil", false));
		gigaSpace.write(new SpaceFruit("apple", "Spain", true));
		
		assertEventuallyPasses(() -> {
			FindIterable<Document> documents = mongoClient.getDatabase("exampleDb").getCollection("spaceFruit").find();

			long count = StreamSupport.stream(documents.spliterator(), false)
									  .count();

			assertEquals("Expected spaceFruit count in mongodb", 2, count);
		});
	}
	
	private void closeSafe(AutoCloseable a) {
		try {
			a.close();
		} catch (Exception e) {
			log.warn("Exception closing {}", a.getClass(), e);
		}
	}

	private void assertEventuallyPasses(Runnable test) throws InterruptedException {
		long timeout = System.currentTimeMillis() + 10_000;
		while (true) {
			try {
				test.run();
				return; // Test passed, return
			} catch (AssertionError e) {
				// Test failed
				if (System.currentTimeMillis() > timeout) {
					throw e;
				} else {
					Thread.sleep(100);
				}
			}
		}
	}

	private ApplicationContext createSingleInstanceAppContext(MongoClient mongo) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoClient", new SimpleMongoClientDatabaseFactory(mongo, "exampleDb"));
		context.refresh();
		return context;
	}

}
