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

import static com.avanza.ymer.BasicDbObjectCompatibility.convertToBasicDBObject;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class BasicDbObjectCompatibilityTest {
	@Test
	public void shouldConvertSimpleBsonDocument() {
		// Arrange
		final var d = new Document(Map.of(
				"key", "value"
		));

		// Act
		final var actual = convertToBasicDBObject(d);

		// Assert
		assertThat(actual.get("key"), equalTo("value"));
	}

	@Test
	public void shouldConvertBsonDocumentWithList() {
		// Arrange
		final var d = new Document(Map.of(
				"list", List.of(1, 2, 3)
		));

		// Act
		final var actual = convertToBasicDBObject(d);

		// Assert
		BasicDBList list = (BasicDBList) actual.get("list");
		assertThat(list, equalTo(List.of(1, 2, 3)));
	}

	@Test
	public void shouldConvertBsonDocumentWithMap() {
		// Arrange
		final var d = new Document(Map.of(
				"map", Map.of(
						"key1", "value1",
						"key2", "value2"
				)
		));

		// Act
		final var actual = convertToBasicDBObject(d);

		// Assert
		BasicDBObject map = (BasicDBObject) actual.get("map");
		assertThat(map.keySet(), equalTo(Set.of("key1", "key2")));
		assertThat(map.get("key1"), equalTo("value1"));
		assertThat(map.get("key2"), equalTo("value2"));
	}

	@Test
	public void shouldConvertComplexNestedBsonDocument() {
		// Arrange
		final var d = Document.parse("{"
				+ "\"accountid\": 3133,"
				+ "\"items\": ["
				+ "  {\"productid\":1,\"quantity\":3},"
				+ "  {\"productid\":2,\"quantity\":4,\"comment\":\"ok\"}"
				+ "],"
				+ "\"categories\":[[9], [7, 8]],"
				+ "\"tags\":{\"tag1\": 1}},"
				+ "\"vat\":null"
				+ "}");

		// Act
		final var actual = convertToBasicDBObject(d);

		// Assert
		assertThat(actual.get("accountid"), equalTo(3133));
		assertThat(actual.get("vat"), equalTo(null));

		final var items = (BasicDBList) actual.get("items");
		assertThat(items.size(), equalTo(2));

		final var item1 = (BasicDBObject) items.get(0);
		assertThat(item1.get("productid"), equalTo(1));
		assertThat(item1.get("quantity"), equalTo(3));

		final var item2 = (BasicDBObject) items.get(1);
		assertThat(item2.get("productid"), equalTo(2));
		assertThat(item2.get("quantity"), equalTo(4));
		assertThat(item2.get("comment"), equalTo("ok"));

		final var categories = (BasicDBList) actual.get("categories");
		assertThat(categories, equalTo(List.of(
				List.of(9),
				List.of(7, 8)
		)));

		final var tags = (BasicDBObject) actual.get("tags");
		assertThat(tags.get("tag1"), equalTo(1));
	}
}
