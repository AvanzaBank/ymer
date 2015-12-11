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
package com.avanza.gs.mongo.mirror;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import com.avanza.gs.mongo.mirror.TransformationUtil.DBObjectMatcher;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class TransformationUtilTest {

	@Test
	public void ensureRemoved() throws Exception {
		DBObject dbObject = BasicDBObjectBuilder.start()
			.add("apa", BasicDBObjectBuilder.start().add("bapa", "hej").get())
			.add("banan", "manan").get();

		new TransformationUtil()
			.ensureRemoved("apa.bapa")
			.apply(dbObject);

		assertNotNull(dbObject.get("apa"));
		assertNull(((DBObject)dbObject.get("apa")).get("bapa"));
		assertNotNull(dbObject.get("banan"));
	}

	@Test
	public void convertValue() throws Exception {
		DBObject dbObject = BasicDBObjectBuilder.start()
			.add("apa", BasicDBObjectBuilder.start().add("bapa", "hej").get())
			.add("banan", "manan").get();

		new TransformationUtil()
			.convertValue("apa.bapa", "hej", "svejs")
			.apply(dbObject);

		assertEquals("svejs", new TransformationUtil.Path("apa.bapa").get(dbObject));
	}

	@Test
	public void convertValue_doesNotConvertNonMatchingValue() throws Exception {
		DBObject dbObject = BasicDBObjectBuilder.start()
				.add("apa", BasicDBObjectBuilder.start().add("bapa", "hej").get())
				.add("banan", "manan").get();

		new TransformationUtil()
			.convertValue("apa.bapa", "hej2", "svejs")
			.apply(dbObject);

		assertEquals("hej", new TransformationUtil.Path("apa.bapa").get(dbObject));
	}

	@Test
	public void move() throws Exception {
		DBObject dbObject = BasicDBObjectBuilder.start()
			.add("apa", BasicDBObjectBuilder.start().add("bapa", "hej").get())
			.add("banan", "manan").get();

		System.out.println(dbObject);

		new TransformationUtil()
			.move("apa.bapa", "fiskpinne.skal")
			.apply(dbObject);

		assertEquals("hej", new TransformationUtil.Path("fiskpinne.skal").get(dbObject));
		assertNotNull(dbObject.get("apa"));
		assertNull(((DBObject)dbObject.get("apa")).get("bapa"));

		System.out.println(dbObject);
	}

	@Test
	public void setValue() throws Exception {
		DBObject dbObject = BasicDBObjectBuilder.start()
				.add("apa", BasicDBObjectBuilder.start().add("bapa", "hej").get())
				.add("banan", "manan").get();

		System.out.println(dbObject);

		new TransformationUtil()
			.setValue("skalfisk", true)
			.apply(dbObject);

		assertNotNull(dbObject.get("skalfisk"));
		assertEquals(Boolean.TRUE, dbObject.get("skalfisk"));

	}

	@Test
	public void pathCanFollowList() throws Exception {
		final BasicDBList list = new BasicDBList();
		list.addAll(Arrays.asList(
				BasicDBObjectBuilder.start().add("_class", "se.avanzabank.customer.registration.domain.tasks.AddressResult").add("address", "fin").get(),
				BasicDBObjectBuilder.start().add("_class", "se.avanzabank.customer.registration.space.accountcreation.AccountCreationTaskResult").add("result", "SUCCESS").get()));
		DBObject dbObject = BasicDBObjectBuilder.start()
				.add("apa", BasicDBObjectBuilder.start().add("bapa", "hej").get())
				.add("completedTasks", list).get();

		TransformationUtil tu = new TransformationUtil().addValue("completedTasks.addressSource", "UNKNOWN", new TransformationUtil.DBObjectMatcher().hasField("_class", "se.avanzabank.customer.registration.domain.tasks.AddressResult"));
	}

	@Test
	public void nullConditional() throws Exception {
		BasicDBObject dbo = (BasicDBObject) JSON.parse("{ \"_id\" : \"A2^1372740169758^709602\" , \"_class\" : \"se.avanzabank.customer.registration.runtime.SpaceAccountRegistration\" , \"customerId\" : { \"_id\" : 209626} , \"accountType\" : \"Investeringssparkonto\" , \"accountId\" : \"2207481\" , \"registrationStatus\" : \"SUCCESS\" , \"pendingTasks\" : [ ] , \"completedTasks\" : [ { \"_class\" : \"se.avanzabank.customer.registration.domain.tasks.AcceptAgreementUserActivityResult\"} , { \"accountId\" : \"2207481\" , \"result\" : \"SUCCESS\" , \"_class\" : \"se.avanzabank.customer.registration.space.accountcreation.AccountCreationTaskResult\"} , { \"result\" : \"SUCCESS\" , \"_class\" : \"se.avanzabank.customer.registration.space.accountcreation.RefreshCustomerAndAccountsTask$RefreshCustomerAndAccountsTaskResult\"} , { \"address\" : { \"addressLines\" : [ \"TERRIERVÄGEN 3\"] , \"city\" : \"HALMSTAD\" , \"zipCode\" : { \"zipCode\" : \"30264\"} , \"careOf\" : { } , \"box\" : { } , \"countryCode\" : { \"countryCode\" : \"SE\"}} , \"_class\" : \"se.avanzabank.customer.registration.domain.tasks.AddressResult\" , \"addressSource\" : \"UNKNOWN\"}] , \"failedTasks\" : [ ] , \"versionID\" : 7 , \"routingKey\" : 1 , \"_formatVersion\" : 2}");
		TransformationUtil tu = new TransformationUtil()
			.addValue("completedTasks.addressSource", "UNKNOWN", new DBObjectMatcher().hasField("_class", "se.avanzabank.customer.registration.domain.tasks.AddressResult"))
			.addValue("completedTasks.address.careOf", "", new DBObjectMatcher().hasField("careOf", null))
			.addValue("completedTasks.address.box", "", new DBObjectMatcher().hasField("box", null));
	}

}
