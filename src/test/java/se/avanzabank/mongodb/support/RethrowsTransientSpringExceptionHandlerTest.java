package se.avanzabank.mongodb.support;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import se.avanzabank.mongodb.support.mirror.DocumentWriteTransientException;


public class RethrowsTransientSpringExceptionHandlerTest {

	private DocumentWriteExceptionHandler handler = new RethrowsTransientSpringExceptionHandler();

	@Test(expected = DocumentWriteTransientException.class)
	public void throwsOnTransientException() throws Exception {
		handler.handleException(new DataAccessResourceFailureException(""), "");
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

}
