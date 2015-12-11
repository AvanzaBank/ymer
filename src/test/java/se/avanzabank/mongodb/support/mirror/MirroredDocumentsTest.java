package se.avanzabank.mongodb.support.mirror;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

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
public class MirroredDocumentsTest {
	
	@Test(expected = NonMirroredTypeException.class)
	public void getMirroredDocumentThrowsIllegalArgumentExceptionForNonMirroredType() throws Exception {
		MirroredDocuments mirroredDocuments = new MirroredDocuments();
		mirroredDocuments.getMirroredDocument(FakeMirroredType.class);
	}
	
	@Test
	public void returnsMirroredDocumentForGivenType() throws Exception {
		MirroredDocument<FakeMirroredType> mirroredDocument = new MirroredDocument<>(FakeMirroredType.class);
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		assertSame(mirroredDocument, mirroredDocuments.getMirroredDocument(FakeMirroredType.class));
	}
	
	@Test
	public void returnsSetOfMirroredTypes() throws Exception {
		MirroredDocument<FakeMirroredType> mirroredDocument = new MirroredDocument<>(FakeMirroredType.class);
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		
		Set<Class<?>> expected = new HashSet<>();
		expected.add(FakeMirroredType.class);
		
		assertEquals(expected, mirroredDocuments.getMirroredTypes());
	}
	
	@Test
	public void returnsSetOfMirroredTypeNames() throws Exception {
		MirroredDocument<FakeMirroredType> mirroredDocument = new MirroredDocument<>(FakeMirroredType.class);
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		
		Set<String> expected = new HashSet<String>();
		expected.add(FakeMirroredType.class.getName());
		
		assertEquals(expected, mirroredDocuments.getMirroredTypeNames());
	}
	
	@Test
	public void returnsAllMirroredDocuments() throws Exception {
		MirroredDocument<FakeMirroredType> mirroredDocument = new MirroredDocument<>(FakeMirroredType.class);
		MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument);
		
		Collection<MirroredDocument<?>> allMirroredDocs = mirroredDocuments.getMirroredDocuments();
		assertEquals(1, allMirroredDocs.size());
		assertSame(mirroredDocument, allMirroredDocs.iterator().next());
	}
	
	@Test
	public void mirroredTypes() throws Exception {
		MirroredDocument<FakeMirroredType> mirroredDocument = new MirroredDocument<>(FakeMirroredType.class);
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
