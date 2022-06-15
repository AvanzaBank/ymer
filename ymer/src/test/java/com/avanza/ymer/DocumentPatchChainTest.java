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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Test;

public class DocumentPatchChainTest {
	
	@Test
	public void appliesPatchesInCorrectOrder() throws Exception {
		BsonDocumentPatch v1ToV2 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch v2ToV3 = mock(BsonDocumentPatch.class);
		when(v1ToV2.patchedVersion()).thenReturn(1);
		when(v2ToV3.patchedVersion()).thenReturn(2);

		DocumentPatchChain<Object> patchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(v2ToV3, v1ToV2));
		
		var patches = patchChain.iterator();
		assertSame(v1ToV2, patches.next());
		assertSame(v2ToV3, patches.next());
		assertFalse(patches.hasNext());
	}
	
	@Test
	public void doesNotAllowHolesInPatchChain() throws Exception {
		BsonDocumentPatch v1ToV2 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch v3ToV4 = mock(BsonDocumentPatch.class);
		when(v1ToV2.patchedVersion()).thenReturn(1);
		when(v3ToV4.patchedVersion()).thenReturn(3);

		assertThrows(IllegalArgumentException.class, () -> new DocumentPatchChain<>(Object.class, Arrays.asList(v3ToV4, v1ToV2)));
	}
	
	@Test
	public void doesNotAllowMoreThanOnePatchFromGivenVersion() throws Exception {
		BsonDocumentPatch v1ToV2a = mock(BsonDocumentPatch.class);
		BsonDocumentPatch v1ToV2b = mock(BsonDocumentPatch.class);
		BsonDocumentPatch v3ToV4 = mock(BsonDocumentPatch.class);
		when(v1ToV2a.patchedVersion()).thenReturn(1);
		when(v1ToV2b.patchedVersion()).thenReturn(1);
		when(v3ToV4.patchedVersion()).thenReturn(2);

		assertThrows(IllegalArgumentException.class, () -> new DocumentPatchChain<>(Object.class, Arrays.asList(v3ToV4, v1ToV2a, v1ToV2b)));
	}
	
	@Test
	public void chainWithThreePatches() throws Exception {
		BsonDocumentPatch p1 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p2 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p3 = mock(BsonDocumentPatch.class);
		when(p1.patchedVersion()).thenReturn(1);
		when(p2.patchedVersion()).thenReturn(2);
		when(p3.patchedVersion()).thenReturn(3);

		new DocumentPatchChain<>(Object.class, Arrays.asList(p3, p1, p2));
	}
	
	@Test
	public void getPatchReturnsThePatchForTheGivenVersion() throws Exception {
		BsonDocumentPatch p2 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p3 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p4 = mock(BsonDocumentPatch.class);
		when(p2.patchedVersion()).thenReturn(2);
		when(p3.patchedVersion()).thenReturn(3);
		when(p4.patchedVersion()).thenReturn(4);

		DocumentPatchChain<Object> documentPatchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(p2, p3, p4));
		assertSame(p2, documentPatchChain.getPatch(2));
		assertSame(p3, documentPatchChain.getPatch(3));
		assertSame(p4, documentPatchChain.getPatch(4));
	}
	
	@Test
	public void throwsIllegalArgumentExceptionWhenAskingForNonExistingPatch() throws Exception {
		BsonDocumentPatch p2 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p3 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p4 = mock(BsonDocumentPatch.class);
		when(p2.patchedVersion()).thenReturn(2);
		when(p3.patchedVersion()).thenReturn(3);
		when(p4.patchedVersion()).thenReturn(4);

		DocumentPatchChain<Object> documentPatchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(p2, p3, p4));
		assertThrows(IllegalArgumentException.class, () -> documentPatchChain.getPatch(5));
	}
	
	@Test
	public void throwsIllegalArgumentExceptionWhenAskingForNonExistingPatch2() throws Exception {
		BsonDocumentPatch p2 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p3 = mock(BsonDocumentPatch.class);
		BsonDocumentPatch p4 = mock(BsonDocumentPatch.class);
		when(p2.patchedVersion()).thenReturn(2);
		when(p3.patchedVersion()).thenReturn(3);
		when(p4.patchedVersion()).thenReturn(4);

		DocumentPatchChain<Object> documentPatchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(p2, p3, p4));
		assertThrows(IllegalArgumentException.class, () -> documentPatchChain.getPatch(1));
	}
		
}
