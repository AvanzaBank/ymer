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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.openspaces.core.GigaSpace;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
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
	public static TestRule spaces = RuleChain.outerRule(mirrorEnvironment).around(pu).around(mirrorPu);

	@Before
	public void setUp() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		mongo = mirrorEnvironment.getMongoTemplate();
		gigaSpace = pu.getClusteredGigaSpace();
	}

	@After
	public void cleanup() {
		mirrorEnvironment.reset();
		gigaSpace.clear(null);
	}

	@Test
	public void mirrorsInsertOfTestSpaceObjects() throws Exception {
		TestSpaceObject o1 = new TestSpaceObject();
		o1.setId("id_23");
		o1.setMessage("mirror_test");

		gigaSpace.write(o1, WriteModifiers.WRITE_ONLY);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));

		assertEventually(() -> mongo.findAll(TestSpaceObject.class), hasItem(o1));
	}

	@Test
	public void testUpdatingSpaceObjectFieldToNullIsSaved() throws Exception {
		TestSpaceObject withMessageSet = new TestSpaceObject();
		withMessageSet.setId("object_id");
		withMessageSet.setMessage("not_null_value");

		gigaSpace.write(withMessageSet, WriteModifiers.WRITE_ONLY);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));
		assertEventually(() -> mongo.findAll(TestSpaceObject.class), hasItem(withMessageSet));

		TestSpaceObject withMessageAsNull = new TestSpaceObject();
		withMessageAsNull.setId("object_id");
		withMessageAsNull.setMessage(null);

		gigaSpace.write(withMessageAsNull, WriteModifiers.UPDATE_OR_WRITE);
		assertEquals(1, gigaSpace.count(new TestSpaceObject()));
		assertEventually(() -> mongo.findAll(TestSpaceObject.class), hasItem(withMessageAsNull));
	}

	@Test
	public void mirrorsUpdatesOfTestSpaceObjects() throws Exception {
		TestSpaceObject inserted = new TestSpaceObject();
		inserted.setId("id_23");
		inserted.setMessage("inserted_mirror_test");
		gigaSpace.write(inserted, WriteModifiers.WRITE_ONLY);

		assertEventually(() -> mongo.findAll(TestSpaceObject.class), hasItem(inserted));

		TestSpaceObject updated = new TestSpaceObject();
		updated.setId("id_23");
		updated.setMessage("updated_mirror_test");
		gigaSpace.write(updated, WriteModifiers.UPDATE_ONLY);

		assertEventually(() -> mongo.findAll(TestSpaceObject.class), hasItem(updated));
	}

	@Test
	public void mirrorsDeletesOfTestSpaceObjects() throws Exception {
		TestSpaceObject inserted = new TestSpaceObject();
		inserted.setId("id_23");
		inserted.setMessage("inserted_mirror_test");
		gigaSpace.write(inserted, WriteModifiers.WRITE_ONLY);

		assertEventually(() -> mongo.findAll(TestSpaceObject.class), hasItem(inserted));

		gigaSpace.takeById(TestSpaceObject.class, "id_23");

		assertEventually(() -> mongo.count(new Query(), TestSpaceObject.class), equalTo(0L));
	}

	@Test
	public void mbeanInvoke() throws Exception {
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		ObjectName nameTemplate = ObjectName
				.getInstance("se.avanzabank.space.mirror:type=DocumentWriteExceptionHandler,name=documentWriteExceptionHandler");
		Set<ObjectName> names = server.queryNames(nameTemplate, null);
		assertThat(names, hasSize(greaterThan(0)));
		server.invoke(names.toArray(new ObjectName[0])[0], "useCatchesAllHandler", null, null);
	}

	@Test
	public void findByTemplate() throws Exception {
		SpaceObjectLoader persister = pu.getPrimaryInstanceApplicationContext(1).getBean(SpaceObjectLoader.class);

		gigaSpace.write(new TestSpaceObject("id1", "m1"));
		gigaSpace.write(new TestSpaceObject("id2", "m2"));
		gigaSpace.write(new TestSpaceObject("id3", "m1"));

		assertEventually(() -> mongo.count(new Query(), TestSpaceObject.class), equalTo(3L));

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
		assertEventually(() -> mongo.findAll(TestSpaceThirdObject.class), hasItem(new TestSpaceThirdObject("1", "a|"))); // prewrite processor prepends a
		assertEquals("|", persister.loadObjects(TestSpaceThirdObject.class, new TestSpaceThirdObject("1", null)).iterator().next().getName()); // a removed by postread processor

		mirrorEnvironment.removeFormatVersion(TestSpaceThirdObject.class, "1");
		assertEquals("b|", persister.loadObjects(TestSpaceThirdObject.class, new TestSpaceThirdObject("1", null)).iterator().next().getName()); // patch adds b to read data (after postread processor)
		assertEventually(() -> mongo.findAll(TestSpaceThirdObject.class), hasItem(new TestSpaceThirdObject("1", "ab|"))); // a is prepended after patching by prewrite processor
		assertEquals("b|", persister.loadObjects(TestSpaceThirdObject.class, new TestSpaceThirdObject("1", null)).iterator().next().getName()); // nothing happens once patch has been applied
		assertEventually(() -> mongo.findAll(TestSpaceThirdObject.class), hasItem(new TestSpaceThirdObject("1", "ab|")));  // same in mongo
	}

	public static <T> void  assertEventually(Callable<T> poller, Matcher<? super T> matcher) {
		await().atMost(7, SECONDS).pollInterval(50, MILLISECONDS).until(poller, matcher);
	}

}
