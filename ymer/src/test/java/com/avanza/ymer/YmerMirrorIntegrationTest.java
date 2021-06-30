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

import static com.avanza.ymer.MongoProbes.containsObject;
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

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.avanza.ymer.test.util.Poller;
import com.avanza.ymer.test.util.Probe;
import com.gigaspaces.client.WriteModifiers;

public class YmerMirrorIntegrationTest {

	private MongoOperations mongo;
	private GigaSpace gigaSpace;
	private static final MirrorEnvironment mirrorEnvironment = new MirrorEnvironment();

	private static final RunningPu pu = PuConfigurers.partitionedPu("classpath:/test-pu.xml")
									   .numberOfBackups(1)
									   .numberOfPrimaries(1)
									   .parentContext(mirrorEnvironment.getMongoClientContext())
									   .configure();

	private static final RunningPu mirrorPu = PuConfigurers.mirrorPu("classpath:/test-mirror-pu.xml")
											   	     .contextProperty("exportExceptionHandlerMBean", "true")
											   	     .parentContext(mirrorEnvironment.getMongoClientContext())
											   	     .configure();

	@ClassRule
	public static TestRule spaces = RuleChain.outerRule(pu).around(mirrorPu);

	@Before
	public void setUp() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		mongo = mirrorEnvironment.getMongoTemplate();
		gigaSpace = pu.getClusteredGigaSpace();
	}

	@After
	public void cleanup() {
		mirrorEnvironment.dropAllMongoCollections();
		gigaSpace.clear(null);
	}

	@Test
	public void mirrorsInsertOfTestSpaceObjects() throws Exception {
		TestSpaceObject o1 = new TestSpaceObject();
		o1.setId("id_23");
		o1.setMessage("mirror_test");

		gigaSpace.write(o1, WriteModifiers.WRITE_ONLY);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));

		assertEventually(containsObject(mongo, equalTo(o1), TestSpaceObject.class));
	}

	@Test
	public void testUpdatingSpaceObjectFieldToNullIsSaved() throws Exception {
		TestSpaceObject withMessageSet = new TestSpaceObject();
		withMessageSet.setId("object_id");
		withMessageSet.setMessage("not_null_value");

		gigaSpace.write(withMessageSet, WriteModifiers.WRITE_ONLY);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));
		assertEventually(containsObject(mongo, equalTo(withMessageSet), TestSpaceObject.class));

		TestSpaceObject withMessageAsNull = new TestSpaceObject();
		withMessageAsNull.setId("object_id");
		withMessageAsNull.setMessage(null);

		gigaSpace.write(withMessageAsNull, WriteModifiers.UPDATE_OR_WRITE);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));
		assertEventually(containsObject(mongo, equalTo(withMessageAsNull), TestSpaceObject.class));
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

	@Test
	public void processingIsApplied() throws Exception {
		SpaceObjectLoader persister = pu.getPrimaryInstanceApplicationContext(1).getBean(SpaceObjectLoader.class);

		gigaSpace.write(new TestSpaceThirdObject("1", "|"));
		assertEventually(containsObject(mongo, equalTo(new TestSpaceThirdObject("1", "a|")), TestSpaceThirdObject.class)); // prewrite processor prepends a
		assertEquals("|", persister.loadObjects(TestSpaceThirdObject.class, new TestSpaceThirdObject("1", null)).iterator().next().getName()); // a removed by postread processor

		mirrorEnvironment.removeFormatVersion(TestSpaceThirdObject.class, "1");
		assertEquals("b|", persister.loadObjects(TestSpaceThirdObject.class, new TestSpaceThirdObject("1", null)).iterator().next().getName()); // patch adds b to read data (after postread processor)
		assertEventually(containsObject(mongo, equalTo(new TestSpaceThirdObject("1", "ab|")), TestSpaceThirdObject.class)); // a is prepended after patching by prewrite processor
		assertEquals("b|", persister.loadObjects(TestSpaceThirdObject.class, new TestSpaceThirdObject("1", null)).iterator().next().getName()); // nothing happens once patch has been applied
		assertEventually(containsObject(mongo, equalTo(new TestSpaceThirdObject("1", "ab|")), TestSpaceThirdObject.class));  // same in mongo
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
