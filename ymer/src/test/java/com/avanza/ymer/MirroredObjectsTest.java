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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * 
 * @author Elias Lindholm (elilin)
 *
 */
public class MirroredObjectsTest {
	
	@Test
	public void getMirroredDocumentThrowsIllegalArgumentExceptionForNonMirroredType() throws Exception {
		MirroredObjects mirroredObjects = new MirroredObjects();
		assertThrows(NonMirroredTypeException.class, () -> mirroredObjects.getMirroredObject(FakeMirroredType.class));
	}
	
	@Test
	public void returnsMirroredDocumentForGivenType() throws Exception {
		BsonDocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredObject = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredObject);
		assertSame(mirroredObject, mirroredObjects.getMirroredObject(FakeMirroredType.class));
	}
	
	@Test
	public void returnsSetOfMirroredTypes() throws Exception {
		BsonDocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredObject = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredObject);
		
		Set<Class<?>> expected = new HashSet<>();
		expected.add(FakeMirroredType.class);
		
		assertEquals(expected, mirroredObjects.getMirroredTypes());
	}
	
	@Test
	public void returnsSetOfMirroredTypeNames() throws Exception {
		BsonDocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredObject = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredObject);
		
		Set<String> expected = new HashSet<>();
		expected.add(FakeMirroredType.class.getName());
		
		assertEquals(expected, mirroredObjects.getMirroredTypeNames());
	}
	
	@Test
	public void returnsAllMirroredDocuments() throws Exception {
		BsonDocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredObject = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredObject);
		
		Collection<MirroredObject<?>> allMirroredDocs = mirroredObjects.getMirroredObjects();
		assertEquals(1, allMirroredDocs.size());
		assertSame(mirroredObject, allMirroredDocs.iterator().next());
	}
	
	@Test
	public void mirroredTypes() throws Exception {
		BsonDocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredObject = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredObject);
		assertTrue(mirroredObjects.isMirroredType(FakeMirroredType.class));
		class NonMirroredType {
			
		}
		assertTrue(mirroredObjects.isMirroredType(FakeMirroredType.class));
		assertFalse(mirroredObjects.isMirroredType(NonMirroredType.class));
	}
	
	static class FakeMirroredType {
		@SpaceRouting
		public Integer getRoutingKey() {
			return null; // Never used
		}
	}

}
