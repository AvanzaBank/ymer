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

import com.avanza.ymer.MirroredDocument;
import com.avanza.ymer.MirroredDocuments;
import com.avanza.ymer.NonMirroredTypeException;
import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * 
 * @author Elias Lindholm (elilin)
 *
 */
public class MirroredDocumentsTest {
	
	@Test(expected = NonMirroredTypeException.class)
	public void getMirroredDocumentThrowsIllegalArgumentExceptionForNonMirroredType() throws Exception {
		MirroredDocuments mirroredDocuments = new MirroredDocuments();
		mirroredDocuments.getMirroredDocument(FakeMirroredType.class);
	}
	
	@Test
	public void returnsMirroredDocumentForGivenType() throws Exception {
		DocumentPatch[] patches = {};
		MirroredDocument<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		assertSame(mirroredDocument, mirroredDocuments.getMirroredDocument(FakeMirroredType.class));
	}
	
	@Test
	public void returnsSetOfMirroredTypes() throws Exception {
		DocumentPatch[] patches = {};
		MirroredDocument<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		
		Set<Class<?>> expected = new HashSet<>();
		expected.add(FakeMirroredType.class);
		
		assertEquals(expected, mirroredDocuments.getMirroredTypes());
	}
	
	@Test
	public void returnsSetOfMirroredTypeNames() throws Exception {
		DocumentPatch[] patches = {};
		MirroredDocument<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		
		Set<String> expected = new HashSet<String>();
		expected.add(FakeMirroredType.class.getName());
		
		assertEquals(expected, mirroredDocuments.getMirroredTypeNames());
	}
	
	@Test
	public void returnsAllMirroredDocuments() throws Exception {
		DocumentPatch[] patches = {};
		MirroredDocument<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		
		Collection<MirroredDocument<?>> allMirroredDocs = mirroredDocuments.getMirroredDocuments();
		assertEquals(1, allMirroredDocs.size());
		assertSame(mirroredDocument, allMirroredDocs.iterator().next());
	}
	
	@Test
	public void mirroredTypes() throws Exception {
		DocumentPatch[] patches = {};
		MirroredDocument<FakeMirroredType> mirroredDocument = MirroredObjectDefinition.create(FakeMirroredType.class).documentPatches(patches).buildMirroredDocument();
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		assertTrue(mirroredDocuments.isMirroredType(FakeMirroredType.class));
		class NonMirroredType {
			
		}
		assertTrue(mirroredDocuments.isMirroredType(FakeMirroredType.class));
		assertFalse(mirroredDocuments.isMirroredType(NonMirroredType.class));
	}
	
	static class FakeMirroredType {
		@SpaceRouting
		public Integer getRoutingKey() {
			return null; // Never used
		}
	}

}
