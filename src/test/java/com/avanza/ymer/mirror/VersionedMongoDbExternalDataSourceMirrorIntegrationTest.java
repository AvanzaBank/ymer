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

import static com.avanza.ymer.mirror.MongoProbes.containsObject;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.openspaces.core.GigaSpace;
import org.springframework.data.mongodb.core.MongoOperations;

import com.avanza.ymer.gs.test.util.PuConfigurers;
import com.avanza.ymer.gs.test.util.RunningPu;
import com.avanza.ymer.test.util.Poller;
import com.avanza.ymer.test.util.Probe;
import com.gigaspaces.client.WriteModifiers;

public class VersionedMongoDbExternalDataSourceMirrorIntegrationTest {
	
	private MongoOperations mongo;
	private GigaSpace gigaSpace;
	private static MirrorEnvironmentRunner mirrorEnviroment = new MirrorEnvironmentRunner(TestSpaceMirrorFactory.getMirroredDocuments());

	private static RunningPu pu = PuConfigurers.partitionedPu("classpath:/mongo-mirror-integration-test-pu.xml")
									   .numberOfBackups(1)
									   .numberOfPrimaries(1)
									   .contextProperty("databasename", mirrorEnviroment.getDatabaseName())
									   .parentContext(mirrorEnviroment.getMongoClientContext())
									   .configure();

	private static RunningPu mirrorPu = PuConfigurers.mirrorPu("classpath:/mongo-mirror-integration-test-mirror-pu.xml")
											   	     .contextProperty("databasename", mirrorEnviroment.getDatabaseName())
											   	     .parentContext(mirrorEnviroment.getMongoClientContext())
											   	     .configure();

	@ClassRule
	public static TestRule spaces = RuleChain.outerRule(mirrorEnviroment).around(pu).around(mirrorPu);

	@Before
	public void setUp() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		mongo = mirrorEnviroment.getMongoTemplate();
		gigaSpace = pu.getClusteredGigaSpace();
	}

	@After
	public void cleanup() {
		mirrorEnviroment.dropAllMongoCollections();
		gigaSpace.clear(null);
	}

	@Test
	public void  mirrorsInsertOfTestSpaceObjects() throws Exception {
		TestSpaceObject o1 = new TestSpaceObject();
		o1.setId("id_23");
		o1.setMessage("mirror_test");

		gigaSpace.write(o1, WriteModifiers.WRITE_ONLY);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));

		assertEventually(containsObject(mongo, equalTo(o1), TestSpaceObject.class));
	}

	@Test
	public void mirrorsUpdatesOfTestSpaceObjects() throws Exception {
		TestSpaceObject inserted = new TestSpaceObject();
		inserted.setId("id_23");
		inserted.setMessage("inserted_mirror_test");
		gigaSpace.write(inserted, WriteModifiers.WRITE_ONLY);

		assertEventually(containsObject(mongo, equalTo(inserted), TestSpaceObject.class));

		TestSpaceObject updated = new TestSpaceObject();
		updated.setId("id_23");
		updated.setMessage("updated_mirror_test");
		gigaSpace.write(updated, WriteModifiers.UPDATE_ONLY);

		assertEventually(containsObject(mongo, equalTo(updated), TestSpaceObject.class));
	}

	@Test
	public void mirrorsDeletesOfTestSpaceObjects() throws Exception {
		TestSpaceObject inserted = new TestSpaceObject();
		inserted.setId("id_23");
		inserted.setMessage("inserted_mirror_test");
		gigaSpace.write(inserted, WriteModifiers.WRITE_ONLY);

		assertEventually(containsObject(mongo, equalTo(inserted), TestSpaceObject.class));

		gigaSpace.takeById(TestSpaceObject.class, "id_23");

		assertEventually(countOf(TestSpaceObject.class, Matchers.equalTo(0)));
	}

	@Test
	public void mbeanInvoke() throws Exception {
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		ObjectName nameTemplate = ObjectName
				.getInstance("se.avanzabank.space.mirror:type=DocumentWriteExceptionHandler,name=documentWriteExceptionHandler");
		Set<ObjectName> names = server.queryNames(nameTemplate, null);
		assertThat(names.size(), is(greaterThan(0)));
		server.invoke(names.toArray(new ObjectName[1])[0], "useCatchesAllHandler", null, null);
	}

	@Test
	public void findByTemplate() throws Exception {
		SpaceObjectLoader persister = pu.getPrimaryInstanceApplicationContext(1).getBean(SpaceObjectLoader.class);

		gigaSpace.write(new TestSpaceObject("id1", "m1"));
		gigaSpace.write(new TestSpaceObject("id2", "m2"));
		gigaSpace.write(new TestSpaceObject("id3", "m1"));

		assertEventually(countOf(TestSpaceObject.class, Matchers.equalTo(3)));

		assertEquals(2, persister.loadObjects(TestSpaceObject.class, new TestSpaceObject(null, "m1")).size());
		assertEquals(1, persister.loadObjects(TestSpaceObject.class, new TestSpaceObject(null, "m2")).size());
		assertEquals(0, persister.loadObjects(TestSpaceObject.class, new TestSpaceObject(null, "m3")).size());
		assertEquals(1, persister.loadObjects(TestSpaceObject.class, new TestSpaceObject("id3", null)).size());
		assertEquals(0, persister.loadObjects(TestSpaceObject.class, new TestSpaceObject("id3", "m2")).size());
		assertEquals(1, persister.loadObjects(TestSpaceObject.class, new TestSpaceObject("id3", "m1")).size());
	}

	private Probe countOf(final Class<?> mirroredType, final Matcher<Integer> countMatcher) {
		return new Probe() {
			int count;

			@Override
			public void sample() {
				count = mongo.findAll(mirroredType).size();
			}

			@Override
			public boolean isSatisfied() {
				return countMatcher.matches(count);
			}

			@Override
			public void describeFailureTo(Description description) {
				description.appendText("Count of " + mirroredType.getName());
				countMatcher.describeTo(description);
			}
		};
	}

	public static void assertEventually(Probe probe) throws InterruptedException {
		new Poller(7000L, 50L).check(probe);
	}

}
