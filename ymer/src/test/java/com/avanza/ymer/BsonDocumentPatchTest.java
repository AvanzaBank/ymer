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

import static java.util.Comparator.comparingInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class BsonDocumentPatchTest {

	@ClassRule
	public static final MirrorEnvironment mirrorEnvironment = new MirrorEnvironment();

	@After
	public void afterEachTest() {
		mirrorEnvironment.reset();
	}

	@Test
	public void shouldApplyDocumentPatchesToDocumentsWithComplexDatatypes() {
		// Arrange
		final var mirroredObjectDefinition = MirroredObjectDefinition
				.create(ExampleObjectWithComplexDatatypes.class)
				.documentPatches(
						new PatchV1ToV2WithDeprecatedBasicDBObject(),
						new PatchV2ToV3WithBsonDocumentPatch(),
						new PatchV3ToV4WithDeprecatedBasicDBObject()
				);
		final var documentCollection = new MongoDocumentCollection(
				mirrorEnvironment.getMongoTemplate().getCollection(mirroredObjectDefinition.collectionName())
		);

		documentCollection.insert(new Document(Map.of(
				// No "_formatVersion" field.
				"_id", 1,
				"listOfMaps", List.of(Map.of("key", "value1")),
				"mapOfLists", Map.of("key", List.of("value1")),
				"accounts", List.of("a1", "a2")
		)));
		documentCollection.insert(new Document(Map.of(
				"_id", 2,
				"_formatVersion", 2,
				"listOfMaps", List.of(Map.of("key", "value2")),
				"mapOfLists", Map.of("key", List.of("value2")),
				"accounts", List.of()
		)));

		final var spaceDataSource = (YmerSpaceDataSource) new YmerFactory(
				() -> mirrorEnvironment.getMongoTemplate().getDb(),
				new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext()),
				List.of(mirroredObjectDefinition)
		).createSpaceDataSource();
		spaceDataSource.setClusterInfo(new ClusterInfo("", 1, 0, 1, 0));

		// Act
		final var loadedObjects = new ArrayList<ExampleObjectWithComplexDatatypes>();
		spaceDataSource.initialDataLoad().forEachRemaining(
				d -> loadedObjects.add((ExampleObjectWithComplexDatatypes) d)
		);

		// Assert
		loadedObjects.sort(comparingInt(d -> d.id));
		final var doc1 = documentCollection.findById(1);
		final var expectedDoc1 = Document.parse("{"
				+ "\"_id\":1,"
				+ "\"_formatVersion\":4,"
				+ "\"patch1applied\":true,"
				+ "\"patch2applied\":true,"
				+ "\"listOfMaps\":["
				+ "  {\"key\":\"value1\"},"
				+ "  {\"patch1listvalue\":\"value1\"},"
				+ "  {\"patch2listvalue\":\"value1\"}"
				+ "],"
				+ "\"mapOfLists\":{"
				+ "  \"key\":[\"value1\"],"
				+ "  \"patch1mapvalue\":[\"value1\"],"
				+ "  \"patch2mapvalue\":[\"value1\"]"
				+ "},"
				+ "\"accounts\":{"
				+ "  \"a1\":\"patched\","
				+ "  \"a2\":\"patched\""
				+ "}"
				+ "}");
		assertThat(doc1, equalTo(expectedDoc1));

		final var obj1 = loadedObjects.get(0);
		assertThat(obj1.id, equalTo(1));
		assertThat(obj1.patch1applied, equalTo(true));
		assertThat(obj1.patch2applied, equalTo(true));
		assertThat(obj1.listOfMaps, equalTo(List.of(
				Map.of("key", "value1"),
				Map.of("patch1listvalue", "value1"),
				Map.of("patch2listvalue", "value1")
		)));
		assertThat(obj1.mapOfLists, equalTo(Map.of(
				"key", List.of("value1"),
				"patch1mapvalue", List.of("value1"),
				"patch2mapvalue", List.of("value1")
		)));
		assertThat(obj1.accounts, equalTo(Map.of(
				"a1", "patched",
				"a2", "patched"
		)));

		final var doc2 = documentCollection.findById(2);
		final var expectedDoc2 = Document.parse("{"
				+ "\"_id\":2,"
				+ "\"_formatVersion\":4,"
				+ "\"patch2applied\":true,"
				+ "\"listOfMaps\":["
				+ "  {\"key\":\"value2\"},"
				+ "  {\"patch2listvalue\":\"value2\"}"
				+ "],"
				+ "\"mapOfLists\":{"
				+ "  \"key\":[\"value2\"],"
				+ "  \"patch2mapvalue\":[\"value2\"]"
				+ "},"
				+ "\"accounts\":{}"
				+ "}");
		assertThat(doc2, equalTo(expectedDoc2));

		final var obj2 = loadedObjects.get(1);
		assertThat(obj2.id, equalTo(2));
		assertThat(obj2.patch1applied, equalTo(false));
		assertThat(obj2.patch2applied, equalTo(true));
		assertThat(obj2.listOfMaps, equalTo(List.of(
				Map.of("key", "value2"),
				Map.of("patch2listvalue", "value2")
		)));
		assertThat(obj2.mapOfLists, equalTo(Map.of(
				"key", List.of("value2"),
				"patch2mapvalue", List.of("value2")
		)));
		assertThat(obj2.accounts, equalTo(Map.of()));
	}

	@SpaceClass
	private static class ExampleObjectWithComplexDatatypes {

		@Id
		public int id;
		public boolean patch1applied;
		public boolean patch2applied;
		public List<Map<String, String>> listOfMaps;
		public Map<String, List<String>> mapOfLists;
		public Map<String, String> accounts;

		@SpaceId
		public int getId() {
			return id;
		}
	}

	private static class PatchV1ToV2WithDeprecatedBasicDBObject implements DocumentPatch {
		@Override
		public void apply(BasicDBObject dbObject) {
			dbObject.put("patch1applied", true);

			// Before: listOfMaps: [{"key":"value0"}]
			// After:  listOfMaps: [{"key":"value0"}, {"patch1listvalue": "value0"}]
			var listOfMaps = (BasicDBList) dbObject.get("listOfMaps");
			listOfMaps.add(new BasicDBObject("patch1listvalue", ((BasicDBObject) listOfMaps.get(0)).get("key")));

			// Before: mapOfLists: {"key":["value0"]}
			// After:  mapOfLists: {"key":["value0"], "patch1mapvalue":["value0"]}
			var mapOfLists = (BasicDBObject) dbObject.get("mapOfLists");
			var list = new BasicDBList();
			list.add(((BasicDBList) mapOfLists.get("key")).get(0));
			mapOfLists.put("patch1mapvalue", list);
		}

		@Override
		public int patchedVersion() {
			return 1;
		}
	}

	private static class PatchV2ToV3WithBsonDocumentPatch implements BsonDocumentPatch {
		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void apply(Document document) {
			document.put("patch2applied", true);

			// Before: listOfMaps: [{"key":"value0"}]
			// After:  listOfMaps: [{"key":"value0"}, {"patch2listvalue": "value0"}]
			var listOfMaps = (List) document.get("listOfMaps");
			listOfMaps.add(Map.of("patch2listvalue", ((Map) listOfMaps.get(0)).get("key")));

			// Before: mapOfLists: {"key":["value0"]}
			// After:  mapOfLists: {"key":["value0"], "patch2mapvalue":["value0"]}
			var mapOfLists = (Map) document.get("mapOfLists");
			mapOfLists.put("patch2mapvalue", List.of(
					((List) mapOfLists.get("key")).get(0)
			));
		}

		@Override
		public int patchedVersion() {
			return 2;
		}
	}

	private static class PatchV3ToV4WithDeprecatedBasicDBObject implements DocumentPatch {
		@Override
		public void apply(BasicDBObject basicDBObject) {
			// Before: accounts: ["a1", "a2"]
			// After:  accounts: {"a1":"patched", "a2":"patched"}
			var accountMap = new BasicDBObject();
			((BasicDBList) basicDBObject.remove("accounts"))
					.forEach(a -> accountMap.put((String) a, "patched"));
			basicDBObject.put("accounts", accountMap);
		}

		@Override
		public int patchedVersion() {
			return 3;
		}
	}
}
