package se.avanzabank.mongodb.support;

public class FakeDocumentWriteExceptionHandler implements DocumentWriteExceptionHandler {

	private final RuntimeException exceptionToThrow;

	/**
	 * Constructs a DocumentWriteExceptionHandler that does nothing.
	 */
	public FakeDocumentWriteExceptionHandler() {
		exceptionToThrow = null;
	}
	

	/**
	 * Constructs a DocumentWriteExceptionHandler that always throws the supplied exception.
	 */
	public FakeDocumentWriteExceptionHandler(RuntimeException exceptionToThrow) {
		this.exceptionToThrow = exceptionToThrow;
	}
	
	@Override
	public void handleException(Exception exception, String operationDescription) {
		if (exceptionToThrow != null) {
			throw exceptionToThrow;
		}
	}

}
