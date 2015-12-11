package se.avanzabank.mongodb.support;

import org.junit.Test;

public class ToggleableDocumentWriteExceptionHandlerTest {

	private FakeDocumentWriteExceptionHandler catchesAllHandler = new FakeDocumentWriteExceptionHandler();
	private FakeDocumentWriteExceptionHandler throwsHandler = new FakeDocumentWriteExceptionHandler(new TestException());
	private ToggleableDocumentWriteExceptionHandler h = ToggleableDocumentWriteExceptionHandler.create(
			throwsHandler, catchesAllHandler);


	@Test(expected = TestException.class)
	public void defaultHandlerThrows() throws Exception {
		h.handleException(new RuntimeException(), "");
	}

	@Test
	public void doesNotThrowAfterSwitchToCatchesAllHandler() throws Exception {
		h.useCatchesAllHandler();
		h.handleException(new RuntimeException(), "");
	}

	@Test(expected = TestException.class)
	public void throwsAfterSwitchBackToDefaultHandler() throws Exception {
		h.useCatchesAllHandler();
		h.useDefaultHandler();
		h.handleException(new RuntimeException(), "");
	}

	public static class TestException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public TestException() {
			super();
		}

	}
}
