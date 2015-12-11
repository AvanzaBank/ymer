package se.avanzabank.mongodb.support;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.mongodb.MongoException;
import com.mongodb.MongoException.Network;

public class CatchesAllDocumentWriteExceptionHandlerTest {
	private DocumentWriteExceptionHandler handler = new CatchesAllDocumentWriteExceptionHandler();

	@Test
	public void doesNotThrowNetworkException() throws Exception {
		try {
			handler.handleException(newMongoNetworkException(), "");
		} catch (RuntimeException e) {
			fail("Exception should not be thrown");
		}
	}

	@Test
	public void doesNotThrowOnRuntimeException() throws Exception {
		try {
			handler.handleException(new RuntimeException(), "");
		} catch (RuntimeException e) {
			fail("Exception should not be thrown");
		}
	}

	private Network newMongoNetworkException() {
		return new MongoException.Network(new IOException());
	}
}
