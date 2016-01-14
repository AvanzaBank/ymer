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
package example.tests;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

import example.domain.SpaceCar;
import example.domain.SpaceFruit;

public class InitialLoadTest {
	
	@Test
	public void intialLoadDemo() throws Exception {
		Fongo fongo = new Fongo("fongoDb");
		fongo.getDB("exampleDb").getCollection("spaceFruit").insert(new BasicDBObject("name", "banana"));
		fongo.getDB("exampleDb").getCollection("spaceFruit").insert(new BasicDBObject("name", "apple"));
		
		
		RunningPu pu = PuConfigurers.partitionedPu("classpath:example-pu.xml")
								    .parentContext(createSingleInstanceAppContext(fongo.getMongo()))
								    .configure();
		
		pu.start();
		
		
		GigaSpace gigaSpace = pu.getClusteredGigaSpace();
		
		assertEquals(0, gigaSpace.count(new SpaceCar()));
		assertEquals(2, gigaSpace.count(new SpaceFruit()));
	}

	private ApplicationContext createSingleInstanceAppContext(MongoClient mongo) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getBeanFactory().registerSingleton("mongoClient", mongo);
		context.refresh();
		return context;
	}

}
