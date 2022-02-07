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

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.avanza.ymer.support.JavaLocalDateTimeReadConverter;
import com.avanza.ymer.support.JavaLocalDateTimeWriteConverter;
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.mongodb.BasicDBObject;

public class ConverterTest {

	private static final LocalDateTime DATE = LocalDateTime.of(1999, 12, 31, 3, 4, 5, 112);
	private static final String DATE_STR = "1999-12-31T03:04:05.000000112";

	private MappingMongoConverter converter;

	@Before
	public void beforeEachTest() {
		final DefaultDbRefResolver ref = new DefaultDbRefResolver(
				new SimpleMongoClientDatabaseFactory("mongodb://localhost/test")
		);
		converter = new MappingMongoConverter(ref, new MongoMappingContext());
		converter.setCustomConversions(new MongoCustomConversions(Arrays.asList(
				new JavaLocalDateTimeReadConverter(),
				new JavaLocalDateTimeWriteConverter()
		)));
		converter.afterPropertiesSet();
	}

	@Test
	public void shouldHandleRead() {
		// Arrange
		BasicDBObject doc = new BasicDBObject();
		doc.put("_id", "id_1");
		doc.put("file", "account");
		doc.put("time", DATE_STR);

		// Act
		ExampleSpaceObj result = converter.read(ExampleSpaceObj.class, doc);

		// Assert
		System.out.println(result);
		assertEquals("id_1", result.getId());
		assertEquals("account", result.getFile());
		assertEquals(DATE, result.getTime());
	}

	@Test
	public void shouldHandleWrite() {
		// Arrange
		ExampleSpaceObj obj = new ExampleSpaceObj();
		obj.setId("id_2");
		obj.setFile("account");
		obj.setTime(DATE);

		// Act
		BasicDBObject doc = new BasicDBObject();
		converter.write(obj, doc);

		// Assert
		System.out.println(doc);
		assertEquals("id_2", doc.get("_id"));
		assertEquals("account", doc.get("file"));
		assertEquals(DATE_STR, doc.get("time"));
	}

	@SpaceClass
	public static class ExampleSpaceObj {

		@Id
		private String id;
		private String file;
		private LocalDateTime time;

		@SpaceId(autoGenerate = true)
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		@SpaceRouting
		public String getFile() {
			return file;
		}

		public void setFile(String file) {
			this.file = file;
		}

		public LocalDateTime getTime() {
			return time;
		}

		public void setTime(LocalDateTime time) {
			this.time = time;
		}

		@Override
		public String toString() {
			return "SpaceFilePublicationEvent{" +
					"id='" + id + '\'' +
					", file='" + file + '\'' +
					", time='" + time + '\'' +
					'}';
		}
	}
}
