package se.avanzabank.mongodb.support;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import se.avanzabank.mongodb.support.mirror.DocumentWriteTransientException;

import com.mongodb.MongoException;
import com.mongodb.MongoException.Network;

/**
 * @author Kristoffer Erlandsson (krierl), kristoffer.erlandsson@avanzabank.se
 */
public class RethrowsTransientDocumentWriteExceptionHandlerTest {

	private DocumentWriteExceptionHandler handler = new RethrowsTransientDocumentWriteExceptionHandler();

	@Test(expected = DocumentWriteTransientException.class)
	public void throwsOnTransientException() throws Exception {
		handler.handleException(newMongoNetworkException(), "");
	}

	@Test
	public void doesNotThrowOnNonTransientException() throws Exception {
		try {
			handler.handleException(new RuntimeException(), "");
		} catch (RuntimeException e) {
			fail("Exception should not be thrown");
		}
	}

	@Test	
	public void throwsOnTransientExceptionByErrorMessage() throws Exception {		
		List<String> transientErrorMessages = Arrays.asList("No replica set members available for query with", "not master");
		for (String message : transientErrorMessages) {
			try {
				handler.handleException(new RuntimeException(message), "");
				fail("Exception should not be thrown for exception with message " + message);
			} catch (DocumentWriteTransientException e) {
			}
		}
	}

	private Network newMongoNetworkException() {
		return new MongoException.Network(new IOException());
	}

}
