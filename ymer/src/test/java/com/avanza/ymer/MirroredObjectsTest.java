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
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.avanza.ymer.MirroredObject;
import com.avanza.ymer.MirroredObjects;
import com.avanza.ymer.NonMirroredTypeException;
import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * 
 * @author Elias Lindholm (elilin)
 *
 */
public class MirroredObjectsTest {
	
	@Test(expected = NonMirroredTypeException.class)
	public void getMirroredDocumentThrowsIllegalArgumentExceptionForNonMirroredType() throws Exception {
		MirroredObjects mirroredObjects = new MirroredObjects();
		mirroredObjects.getMirroredDocument(FakeMirroredType.class);
	}
	
	@Test
	public void returnsMirroredDocumentForGivenType() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredDocument);
		assertSame(mirroredDocument, mirroredObjects.getMirroredDocument(FakeMirroredType.class));
	}
	
	@Test
	public void returnsSetOfMirroredTypes() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredDocument);
		
		Set<Class<?>> expected = new HashSet<>();
		expected.add(FakeMirroredType.class);
		
		assertEquals(expected, mirroredObjects.getMirroredTypes());
	}
	
	@Test
	public void returnsSetOfMirroredTypeNames() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredDocument);
		
		Set<String> expected = new HashSet<String>();
		expected.add(FakeMirroredType.class.getName());
		
		assertEquals(expected, mirroredObjects.getMirroredTypeNames());
	}
	
	@Test
	public void returnsAllMirroredDocuments() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredDocument);
		
		Collection<MirroredObject<?>> allMirroredDocs = mirroredObjects.getMirroredDocuments();
		assertEquals(1, allMirroredDocs.size());
		assertSame(mirroredDocument, allMirroredDocs.iterator().next());
	}
	
	@Test
	public void mirroredTypes() throws Exception {
		DocumentPatch[] patches = {};
		MirroredObject<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredObjects mirroredObjects = new MirroredObjects(mirroredDocument);
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
