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

import static com.avanza.ymer.MirroredObjectDefinitionsOverride.fromSystemProperties;
import static com.avanza.ymer.PersistedInstanceIdUtil.getInstanceIdFieldName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.springframework.data.mongodb.MongoCollectionUtils;

import com.avanza.gs.test.JVMGlobalLus;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.mongodb.BasicDBObject;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MirroredObjectTest {

	@Test
	public void cannotMirrorTypesWithNoRoutingMethod() throws Exception {
		class InvalidSpaceObject {
			@SuppressWarnings("unused")
			public Integer fooMethod() {
				return null; // Never used
			}
		}
		DocumentPatch[] patches = {};
		assertThrows(IllegalArgumentException.class, () -> MirroredObjectDefinition.create(InvalidSpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride()));
	}

	@Test
	public void routingMethodIsSpaceIdMethodIfNoSpaceRoutingAnnotationPresent() throws Exception {
		class SpaceObject {
			@SpaceId
			public Integer routingKey() {
				return 21;
			}
		}
		DocumentPatch[] patches = {};
		MirroredObject<SpaceObject> mirroredObject = MirroredObjectDefinition.create(SpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals(21, mirroredObject.getRoutingKey(new SpaceObject()));
	}

	@Test
	public void returnsGigaSpaceApplicableRoutingKey() throws Exception {
		class SpaceObject {
			@SpaceId(autoGenerate = true)
			public String routingKey() {
				return "A1^1403854928211^257";
			}
		}
		DocumentPatch[] patches = {};
		MirroredObject<SpaceObject> mirroredObject = MirroredObjectDefinition.create(SpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals("A1", mirroredObject.getRoutingKey(new SpaceObject()));
	}

	public static class MySpaceObject {
		String id;
		public MySpaceObject() {}
		@SpaceId(autoGenerate = true)
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
	}
	
	@Test
	public void decidesCorrectRouingKey() throws Exception {
		EmbeddedSpace embeddedSpace = new EmbeddedSpace();
		GigaSpace gigaSpace = embeddedSpace.gigaSpace();
		DocumentPatch[] patches = {};

		MirroredObject<MySpaceObject> mirroredObject = MirroredObjectDefinition.create(MySpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		gigaSpace.writeMultiple(new MySpaceObject[] {new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject()});
		for (MySpaceObject spaceObject : gigaSpace.readMultiple(new MySpaceObject())) {
			assertEquals((String)mirroredObject.getRoutingKey(spaceObject), 1, Math.abs(mirroredObject.getRoutingKey(spaceObject).hashCode()) % 2);
		}

		embeddedSpace.destroy();
	}

	@Test
	public void usesSpaceRoutingAnnotationToFindRoutingKey() throws Exception {
		class SpaceObject {
			@SpaceRouting
			public Integer routing() {
				return 21;
			}

			@SpaceId
			public Integer id() {
				return 19;
			}
		}
		DocumentPatch[] patches = {};
		MirroredObject<SpaceObject> mirroredObject = MirroredObjectDefinition.create(SpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals(21, mirroredObject.getRoutingKey(new SpaceObject()));
	}


	@Test
	public void patchesAreAppliedInCorrectOrder() throws Exception {
		List<FakePatch> appliedPatchesInAppliedOrder = new ArrayList<>();
		FakePatch patch2 = new FakePatch(2, appliedPatchesInAppliedOrder);
		FakePatch patch1 = new FakePatch(1, appliedPatchesInAppliedOrder);
		DocumentPatch[] patches = { patch2, patch1 };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document doc = new Document();
		document.patch(doc);
		assertEquals(2, appliedPatchesInAppliedOrder.size());
		assertSame(patch1, appliedPatchesInAppliedOrder.get(0));
		assertSame(patch2, appliedPatchesInAppliedOrder.get(1));
	}


	@Test
	public void currentVersionIsOneMoreThanLatestPathchedVersion() throws Exception {
		DocumentPatch[] patches = { new FakePatch(1), new FakePatch(2) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals(3, document.getCurrentVersion());
	}

	@Test
	public void currentVersionIsOnIfeDocumentHasNoPatches() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals(1, document.getCurrentVersion());
	}

	@Test
	public void documentRequiresPatchingIfVersionToOld() throws Exception {
		DocumentPatch[] patches = { new FakePatch(1), new FakePatch(2) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document dbObject = new Document();
		document.setDocumentVersion(dbObject, document.getCurrentVersion() - 1);

		assertTrue(document.requiresPatching(dbObject));
	}

	@Test
	public void documentDoesNotRequirePatchingIfDocumentIsUpToDate() throws Exception {
		DocumentPatch[] patches = { new FakePatch(1), new FakePatch(2) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document dbObject = new Document();
		document.setDocumentVersion(dbObject, document.getCurrentVersion());

		assertFalse(document.requiresPatching(dbObject));
	}

	@Test
	public void cannotPatchDocumentThatAreNewerThanLatestKnownVersion() throws Exception {
		DocumentPatch[] patches = { new FakePatch(1), new FakePatch(2) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document dbObject = new Document();
		document.setDocumentVersion(dbObject, document.getCurrentVersion() + 1);
		assertThrows(UnknownDocumentVersionException.class, () -> document.requiresPatching(dbObject));
	}

	@Test
	public void cannotPatchDocumentThatAreOlderThanOldestKnownVersion() throws Exception {
		DocumentPatch[] patches = { new FakePatch(2), new FakePatch(3) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document dbObject = new Document();
		document.setDocumentVersion(dbObject, 1);

		assertThrows(UnknownDocumentVersionException.class, () -> document.requiresPatching(dbObject));
	}

	@Test
	public void patchingWhenNoPatchesExistsThrowsIllegalArgumentException() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document doc = new Document();
		assertThrows(IllegalArgumentException.class, () -> document.patch(doc));
	}

	@Test
	public void patchedDocumentHasLatestDocFormatVersion() throws Exception {
		DocumentPatch[] patches = { new FakePatch(1), new FakePatch(2) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document dbObject = new Document();
		Document patched = document.patch(dbObject);

		assertEquals(3, document.getDocumentVersion(patched));
	}

	@Test
	public void appliesAllPatchesIfDocumentIsOnVersionOne() throws Exception {
		FakePatch patch1 = new FakePatch(1);
		FakePatch patch2 = new FakePatch(2);
		DocumentPatch[] patches = { patch1, patch2 };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document doc = new Document();

		document.patch(doc);
		assertTrue(patch1.applied);
		assertTrue(patch2.applied);
	}

	@Test
	public void onlyAppliesAppropriatePatches() throws Exception {
		FakePatch patch1 = new FakePatch(1);
		FakePatch patch2 = new FakePatch(2);
		DocumentPatch[] patches = { patch1, patch2 };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document doc = new Document();
		document.setDocumentVersion(doc, 2);
		document.patch(doc);
		assertFalse(patch1.applied);
		assertTrue(patch2.applied);
	}

	@Test
	public void throwsUnknownDocumentVersionExceptionIfFormatVersionIsNewerThanCurrentFormatVersion() throws Exception {
		DocumentPatch[] patches = { new FakePatch(1) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document doc = new Document();
		document.setDocumentVersion(doc, 3);

		assertThrows(UnknownDocumentVersionException.class, () -> document.requiresPatching(doc));
	}

	@Test
	public void knownVersions() throws Exception {
		DocumentPatch[] patches = { new FakePatch(2), new FakePatch(3) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertFalse("Versions before oldest known version are not known", document.isKnownVersion(1));
		assertTrue(document.isKnownVersion(2));
		assertTrue(document.isKnownVersion(3));
		assertTrue(document.isKnownVersion(4));
		assertFalse("Versions after current version is not known", document.isKnownVersion(5));
	}

	@Test
	public void collectionName() throws Exception {
		DocumentPatch[] patches = { new FakePatch(2), new FakePatch(3) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		 assertEquals(MongoCollectionUtils.getPreferredCollectionName(document.getMirroredType()), document.getCollectionName());
	}

    @Test
    public void setCollectionName() throws Exception{
        String collectionName = "fakeCollection";
        MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class)
        																    .collectionName(collectionName)
        																    .documentPatches(new FakePatch(2), new FakePatch(3))
        																    .buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
        assertEquals(collectionName, document.getCollectionName());

    }

	@Test
	public void oldestVersionIsCurrentVersionIfNoPatchesExists() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals(document.getCurrentVersion(), document.getOldestKnownVersion());
	}

	@Test
	public void oldestVersionIsOldestPatchedVersionIfPatchesExists() throws Exception {
		DocumentPatch[] patches = { new FakePatch(2), new FakePatch(3) };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		assertEquals(2, document.getOldestKnownVersion());
	}

	@Test
	public void patchOneVersionAppliesOnlyASinglePatch() throws Exception {
		FakePatch patch1 = new FakePatch(1);
		FakePatch patch2 = new FakePatch(2);
		FakePatch patch3 = new FakePatch(3);
		DocumentPatch[] patches = { patch1, patch2, patch3 };
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());

		Document doc = new Document();
		document.setDocumentVersion(doc, 2);

		document.patchToNextVersion(doc);

		assertFalse(patch1.applied);
		assertTrue(patch2.applied);
		assertFalse(patch3.applied);
	}

	@Test
	public void setsRoutingFieldForRoutedDocumentLoad() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class).loadDocumentsRouted(true).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document dbObject = new Document();

		document.setDocumentAttributes(dbObject, new MirroredType(23), null);
		assertEquals(23, dbObject.get(MirroredObject.DOCUMENT_ROUTING_KEY));
	}

	@Test
	public void canDetermineRoutingKeyFromStringRoutingValue() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<RoutedType> document = MirroredObjectDefinition.create(RoutedType.class).loadDocumentsRouted(true).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document dbObject = new Document();

		document.setDocumentAttributes(dbObject, new RoutedType(23, "bananskal"), null);
		assertEquals("bananskal".hashCode(), dbObject.get(MirroredObject.DOCUMENT_ROUTING_KEY));
	}

	@Test
	public void setsInstanceIdAndRoutingKeyForPersistInstanceId() throws Exception {
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class)
				.persistInstanceId()
				.and()
				.buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document dbObject = new Document();

		int routingKey = 23;
		InstanceMetadata metadata = new InstanceMetadata(2, null);
		document.setDocumentAttributes(dbObject, new MirroredType(routingKey), metadata);
		assertEquals(routingKey, dbObject.get(MirroredObject.DOCUMENT_ROUTING_KEY));
		assertEquals(2, dbObject.get(getInstanceIdFieldName(2)));
	}

	@Test
	public void setsNextInstanceIdAndRoutingKeyForPersistInstanceId() throws Exception {
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class)
				.persistInstanceId()
				.and()
				.buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document dbObject = new Document();

		int routingKey = 23;
		InstanceMetadata metadata = new InstanceMetadata(2, 3);
		document.setDocumentAttributes(dbObject, new MirroredType(routingKey), metadata);
		assertEquals(routingKey, dbObject.get(MirroredObject.DOCUMENT_ROUTING_KEY));
		assertEquals(2, dbObject.get(getInstanceIdFieldName(2)));
		assertEquals(3, dbObject.get(getInstanceIdFieldName(3)));
	}

	@Test
	public void doesNotSetInstanceIdWhenNull() throws Exception {
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class)
				.persistInstanceId()
				.and()
				.buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		int routingKey = 23;

		Document nullNumberOfPartitions = new Document();
		InstanceMetadata nullNumberOfPartitionsMetadata = new InstanceMetadata(null, null);
		document.setDocumentAttributes(nullNumberOfPartitions, new MirroredType(routingKey), nullNumberOfPartitionsMetadata);
		assertEquals(routingKey, nullNumberOfPartitions.get(MirroredObject.DOCUMENT_ROUTING_KEY));
		assertNoInstanceIdFieldsAreSet(nullNumberOfPartitions);
	}

	@Test
	public void willAlwaysCalculateNextNumberOfPartitionsIfSet() throws Exception {
		MirroredObject<MirroredType> document = MirroredObjectDefinition.create(MirroredType.class)
				.persistInstanceId()
				.and()
				.buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		Document dbObject = new Document();

		int routingKey = 23;
		document.setDocumentAttributes(dbObject, new MirroredType(routingKey), new InstanceMetadata(null, 2));
		assertEquals(routingKey, dbObject.get(MirroredObject.DOCUMENT_ROUTING_KEY));
		assertEquals(2, dbObject.get(getInstanceIdFieldName(2)));
	}

	@Test
	public void canOverrideDefinition() throws Exception {
		MirroredObjectDefinition<RoutedType> definition = MirroredObjectDefinition.create(RoutedType.class)
				.loadDocumentsRouted(true)
				.writeBackPatchedDocuments(true)
				.persistInstanceId()
				.and()
				.excludeFromInitialLoad(true);

		assertTrue(definition.buildMirroredDocument(fromSystemProperties()).loadDocumentsRouted());
		assertTrue(definition.buildMirroredDocument(fromSystemProperties()).writeBackPatchedDocuments());
		assertTrue(definition.buildMirroredDocument(fromSystemProperties()).excludeFromInitialLoad());
		assertTrue(definition.buildMirroredDocument(fromSystemProperties()).persistInstanceId());

		System.setProperty("ymer.com.avanza.ymer.MirroredObjectTest.RoutedType.loadDocumentsRouted", "false");
		System.setProperty("ymer.com.avanza.ymer.MirroredObjectTest.RoutedType.writeBackPatchedDocuments", "false");
		System.setProperty("ymer.com.avanza.ymer.MirroredObjectTest.RoutedType.excludeFromInitialLoad", "false");
		System.setProperty("ymer.com.avanza.ymer.MirroredObjectTest.RoutedType.persistInstanceId", "false");

		assertFalse(definition.buildMirroredDocument(fromSystemProperties()).loadDocumentsRouted());
		assertFalse(definition.buildMirroredDocument(fromSystemProperties()).writeBackPatchedDocuments());
		assertFalse(definition.buildMirroredDocument(fromSystemProperties()).excludeFromInitialLoad());
		assertFalse(definition.buildMirroredDocument(fromSystemProperties()).persistInstanceId());
	}

	private static void assertNoInstanceIdFieldsAreSet(Document document) {
		Set<String> fields = document.keySet().stream().filter(it -> it.startsWith(MirroredObject.DOCUMENT_INSTANCE_ID)).collect(Collectors.toSet());
		assertTrue("Expected no instance id fields to exist, but found " + fields, fields.isEmpty());
	}

	static class MirroredType {
		private final int routingKey;

		public MirroredType() {
			this(1);
		}
		public MirroredType(int routingKey) {
			this.routingKey = routingKey;
		}

		@SpaceRouting
		public Integer getRoutingKey() {
			return routingKey;
		}
	}

	static class RoutedType {
		private final Integer id;
		private final String myRoute;
		private RoutedType(Integer id, String myRoute) {
			this.id = id;
			this.myRoute = myRoute;
		}
		@SpaceId(autoGenerate = false)
		public final Integer getId() {
			return id;
		}
		@SpaceRouting
		public final String getMyRoute() {
			return myRoute;
		}
	}

	static class FakePatch implements DocumentPatch {

		private final int patchedVersion;
		public boolean applied = false;
		private final List<FakePatch> appliedPatches;

		public FakePatch(int patchedVersion) {
			this(patchedVersion, new ArrayList<>());
		}

		public FakePatch(int patchedVersion, List<FakePatch> appliedPatches) {
			this.patchedVersion = patchedVersion;
			this.appliedPatches = appliedPatches;
		}

		@Override
		public void apply(BasicDBObject dbObject) {
			applied = true;
			appliedPatches.add(this);
		}

		@Override
		public int patchedVersion() {
			return patchedVersion;
		}

	}
	
	private static class EmbeddedSpace {
		private static final AtomicInteger count = new AtomicInteger();
		EmbeddedSpaceConfigurer configurer = new EmbeddedSpaceConfigurer("MirroredObjectTest-" + count.incrementAndGet()).lookupGroups(JVMGlobalLus.getLookupGroupName());
		GigaSpace space = new GigaSpaceConfigurer(configurer.create()).create();
		public GigaSpace gigaSpace() {
			return space;
		}
		public void destroy() {
			this.configurer.close();
		}
	}

}
