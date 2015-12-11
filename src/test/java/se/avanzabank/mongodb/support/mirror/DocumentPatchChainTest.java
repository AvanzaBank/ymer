package se.avanzabank.mongodb.support.mirror;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;
import org.mockito.Mockito;

public class DocumentPatchChainTest {
	
	@Test
	public void appliesPatchesInCorrectOrder() throws Exception {
		DocumentPatch v1ToV2 = Mockito.mock(DocumentPatch.class);
		DocumentPatch v2ToV3 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(v1ToV2.patchedVersion()).toReturn(1);
		Mockito.stub(v2ToV3.patchedVersion()).toReturn(2);

		DocumentPatchChain<Object> patchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(v2ToV3, v1ToV2));
		
		Iterator<DocumentPatch> patches = patchChain.iterator();
		assertSame(v1ToV2, patches.next());
		assertSame(v2ToV3, patches.next());
		assertFalse(patches.hasNext());
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void doesNotAllowHolesInPatchChain() throws Exception {
		DocumentPatch v1ToV2 = Mockito.mock(DocumentPatch.class);
		DocumentPatch v3ToV4 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(v1ToV2.patchedVersion()).toReturn(1);
		Mockito.stub(v3ToV4.patchedVersion()).toReturn(3);

		new DocumentPatchChain<>(Object.class, Arrays.asList(v3ToV4, v1ToV2));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void doesNotAllowMoreThanOnePatchFromGivenVersion() throws Exception {
		DocumentPatch v1ToV2a = Mockito.mock(DocumentPatch.class);
		DocumentPatch v1ToV2b = Mockito.mock(DocumentPatch.class);
		DocumentPatch v3ToV4 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(v1ToV2a.patchedVersion()).toReturn(1);
		Mockito.stub(v1ToV2b.patchedVersion()).toReturn(1);
		Mockito.stub(v3ToV4.patchedVersion()).toReturn(2);

		new DocumentPatchChain<>(Object.class, Arrays.asList(v3ToV4, v1ToV2a, v1ToV2b));
	}
	
	@Test
	public void chaingWithThreePatches() throws Exception {
		DocumentPatch p1 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p2 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p3 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(p1.patchedVersion()).toReturn(1);
		Mockito.stub(p2.patchedVersion()).toReturn(2);
		Mockito.stub(p3.patchedVersion()).toReturn(3);

		new DocumentPatchChain<>(Object.class, Arrays.asList(p3, p1, p2));
	}
	
	@Test
	public void getPatchReturnsThePatchForTheGivenVersion() throws Exception {
		DocumentPatch p2 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p3 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p4 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(p2.patchedVersion()).toReturn(2);
		Mockito.stub(p3.patchedVersion()).toReturn(3);
		Mockito.stub(p4.patchedVersion()).toReturn(4);

		DocumentPatchChain<Object> documentPatchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(p2, p3, p4));
		assertSame(p2, documentPatchChain.getPatch(2));
		assertSame(p3, documentPatchChain.getPatch(3));
		assertSame(p4, documentPatchChain.getPatch(4));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void throwsIllegalArgumentExceptionWhenAskingForNonExistingPatch() throws Exception {
		DocumentPatch p2 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p3 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p4 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(p2.patchedVersion()).toReturn(2);
		Mockito.stub(p3.patchedVersion()).toReturn(3);
		Mockito.stub(p4.patchedVersion()).toReturn(4);

		DocumentPatchChain<Object> documentPatchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(p2, p3, p4));
		assertSame(p2, documentPatchChain.getPatch(5));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void throwsIllegalArgumentExceptionWhenAskingForNonExistingPatch2() throws Exception {
		DocumentPatch p2 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p3 = Mockito.mock(DocumentPatch.class);
		DocumentPatch p4 = Mockito.mock(DocumentPatch.class);
		Mockito.stub(p2.patchedVersion()).toReturn(2);
		Mockito.stub(p3.patchedVersion()).toReturn(3);
		Mockito.stub(p4.patchedVersion()).toReturn(4);

		DocumentPatchChain<Object> documentPatchChain = new DocumentPatchChain<>(Object.class, Arrays.asList(p2, p3, p4));
		assertSame(p2, documentPatchChain.getPatch(1));
	}
		
}
