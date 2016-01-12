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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.springframework.data.mongodb.MongoCollectionUtils;

import com.avanza.ymer.gs.test.util.JVMGlobalLus;
import com.avanza.ymer.mirror.MirroredDocument.Flag;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.mongodb.BasicDBObject;

/**
 *
 * @author Elias Lindholm (elilin)
 *
 */
public class MirroredDocumentTest {

	@Test(expected=IllegalArgumentException.class)
	public void cannotMirrorTypesWithNoRoutingMethod() throws Exception {
		class InvalidSpaceObject {
			@SuppressWarnings("unused")
			public Integer fooMethod() {
				return null; // Never used
			}
		}
		new MirroredDocument<>(InvalidSpaceObject.class);
	}

	@Test
	public void routingMethodIsSpaceIdMethodIfNoSpaceRoutingAnnotationPresent() throws Exception {
		class SpaceObject {
			@SpaceId
			public Integer routingKey() {
				return 21;
			}
		}
		MirroredDocument<SpaceObject> mirroredDocument = new MirroredDocument<>(SpaceObject.class);
		assertEquals(21, mirroredDocument.getRoutingKey(new SpaceObject()));
	}

	@Test
	public void returnsGigaSpaceApplicableRoutingKey() throws Exception {
		class SpaceObject {
			@SpaceId(autoGenerate = true)
			public String routingKey() {
				return "A1^1403854928211^257";
			}
		}
		MirroredDocument<SpaceObject> mirroredDocument = new MirroredDocument<>(SpaceObject.class);
		assertEquals("A1", mirroredDocument.getRoutingKey(new SpaceObject()));
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

		MirroredDocument<MySpaceObject> mirroredDocument = new MirroredDocument<>(MySpaceObject.class);

		gigaSpace.writeMultiple(new MySpaceObject[] {new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject(),new MySpaceObject()});
		for (MySpaceObject spaceObject : gigaSpace.readMultiple(new MySpaceObject())) {
			assertEquals((String)mirroredDocument.getRoutingKey(spaceObject), 1, Math.abs(mirroredDocument.getRoutingKey(spaceObject).hashCode()) % 2);
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
		MirroredDocument<SpaceObject> mirroredDocument = new MirroredDocument<>(SpaceObject.class);
		assertEquals(21, mirroredDocument.getRoutingKey(new SpaceObject()));
	}


	@Test
	public void patchesAreAppliedInCorrectOrder() throws Exception {
		List<FakePatch> appliedPatchesInAppliedOrder = new ArrayList<>();
		FakePatch patch2 = new FakePatch(2, appliedPatchesInAppliedOrder);
		FakePatch patch1 = new FakePatch(1, appliedPatchesInAppliedOrder);
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, patch2, patch1);

		BasicDBObject dbObject = new BasicDBObject();
		document.patch(dbObject);
		assertEquals(2, appliedPatchesInAppliedOrder.size());
		assertSame(patch1, appliedPatchesInAppliedOrder.get(0));
		assertSame(patch2, appliedPatchesInAppliedOrder.get(1));
	}


	@Test
	public void currentVersionIsOneMoreThanLatestPathchedVersion() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(1), new FakePatch(2));
		assertEquals(3, document.getCurrentVersion());
	}

	@Test
	public void currentVersionIsOnIfeDocumentHasNoPatches() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class);
		assertEquals(1, document.getCurrentVersion());
	}

	@Test
	public void documentRequiresPatchingIfVersionToOld() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(1), new FakePatch(2));

		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, document.getCurrentVersion() - 1);

		assertTrue(document.requiresPatching(dbObject));
	}

	@Test
	public void documentDoesNotRequirePatchingIfDocumentIsUpToDate() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(1), new FakePatch(2));

		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, document.getCurrentVersion());

		assertFalse(document.requiresPatching(dbObject));
	}

	@Test(expected = UnknownDocumentVersionException.class)
	public void cannotPatchDocumentThatAreNewerThanLatestKnownVersion() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(1), new FakePatch(2));

		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, document.getCurrentVersion() + 1);
		document.requiresPatching(dbObject);
	}

	@Test(expected = UnknownDocumentVersionException.class)
	public void cannotPatchDocumentThatAreOlderThanOldestKnownVersion() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(2), new FakePatch(3));

		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, 1);

		document.requiresPatching(dbObject);
	}

	@Test(expected = IllegalArgumentException.class)
	public void patchingWhenNoPatchesExistsThrowsIllegalArgumentException() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class);
		BasicDBObject dbObject = new BasicDBObject();
		BasicDBObject patched = document.patch(dbObject);
		assertEquals(dbObject, patched);
	}

	@Test
	public void patchedDocumentHasLatestDocFormatVersion() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(1), new FakePatch(2));

		BasicDBObject dbObject = new BasicDBObject();
		BasicDBObject patched = document.patch(dbObject);

		assertEquals(3, document.getDocumentVersion(patched));
	}

	@Test
	public void appliesAllPathechesIfDocumentIsOnVersionOne() throws Exception {
		FakePatch patch1 = new FakePatch(1);
		FakePatch patch2 = new FakePatch(2);
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, patch1, patch2);
		BasicDBObject dbObject = new BasicDBObject();

		BasicDBObject patched = document.patch(dbObject);
		assertTrue(patch1.applied);
		assertTrue(patch2.applied);
	}

	@Test
	public void onlyAppliesAppropriatePatches() throws Exception {
		FakePatch patch1 = new FakePatch(1);
		FakePatch patch2 = new FakePatch(2);
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, patch1, patch2);
		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, 2);
		document.patch(dbObject);
		assertFalse(patch1.applied);
		assertTrue(patch2.applied);
	}

	@Test(expected = UnknownDocumentVersionException.class)
	public void throwsUnkownDocumentVersionExceptionIfFormatVersionIsNewerThanCurrentFormatVersion() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(1));
		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, 3);

		document.requiresPatching(dbObject);
	}

	@Test
	public void knownVersions() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(2), new FakePatch(3));
		assertFalse("Versions before oldest known version are not known", document.isKnownVersion(1));
		assertTrue(document.isKnownVersion(2));
		assertTrue(document.isKnownVersion(3));
		assertTrue(document.isKnownVersion(4));
		assertFalse("Versions after current version is not known", document.isKnownVersion(5));
	}

	@Test
	public void collectionName() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(2), new FakePatch(3));
		 assertEquals(MongoCollectionUtils.getPreferredCollectionName(document.getMirroredType()), document.getCollectionName());
	}

    @Test
    public void setCollectionName() throws Exception{
        String collectionName = "fakeCollection";
        MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, collectionName, new FakePatch(2), new FakePatch(3));
        assertEquals(collectionName, document.getCollectionName());

    }

	@Test
	public void oldestVersionIsCurrentVersionIfNoPatchesExists() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class);
		assertEquals(document.getCurrentVersion(), document.getOldestKnownVersion());
	}

	@Test
	public void oldestVersionIsOldestPatchedVersionIfPatchesExists() throws Exception {
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, new FakePatch(2), new FakePatch(3));
		assertEquals(2, document.getOldestKnownVersion());
	}

	@Test
	public void patchOneVersionAppliesOnlyASinglePatch() throws Exception {
		FakePatch patch1 = new FakePatch(1);
		FakePatch patch2 = new FakePatch(2);
		FakePatch patch3 = new FakePatch(3);
		MirroredDocument<MirroredType> document = new MirroredDocument<>(MirroredType.class, patch1, patch2, patch3);

		BasicDBObject dbObject = new BasicDBObject();
		document.setDocumentVersion(dbObject, 2);

		document.patchToNextVersion(dbObject);

		assertFalse(patch1.applied);
		assertTrue(patch2.applied);
		assertFalse(patch3.applied);
	}

	@Test
	public void setsRoutingFieldForRoutedDocumentLoad() throws Exception {
		MirroredDocument<MirroredType> document = MirroredDocument.createDocument(MirroredType.class, EnumSet.<Flag>of(Flag.LOAD_DOCUMENTS_ROUTED));
		BasicDBObject dbObject = new BasicDBObject();

		document.setDocumentAttributes(dbObject, new MirroredType(23));
		assertEquals(23, dbObject.get(MirroredDocument.DOCUMENT_ROUTING_KEY));
	}

	@Test
	public void canDetermineRoutingKeyFromStringRoutingValue() throws Exception {
		MirroredDocument<RoutedType> document = MirroredDocument.createDocument(RoutedType.class, EnumSet.<Flag>of(Flag.LOAD_DOCUMENTS_ROUTED));
		BasicDBObject dbObject = new BasicDBObject();

		document.setDocumentAttributes(dbObject, new RoutedType(23, "bananskal"));
		assertEquals("bananskal".hashCode(), dbObject.get(MirroredDocument.DOCUMENT_ROUTING_KEY));
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
			this(patchedVersion, new ArrayList<FakePatch>());
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
		EmbeddedSpaceConfigurer configurer = new EmbeddedSpaceConfigurer("MirroredDocumentTest-" + count.incrementAndGet()).lookupGroups(JVMGlobalLus.getLookupGroupName());
		GigaSpace space = new GigaSpaceConfigurer(configurer.create()).create();
		public GigaSpace gigaSpace() {
			return space;
		}
		public void destroy() {
			this.configurer.close();
		}
	}

}
