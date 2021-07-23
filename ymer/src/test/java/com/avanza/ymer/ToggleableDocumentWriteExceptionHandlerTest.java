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

import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class ToggleableDocumentWriteExceptionHandlerTest {

	private final FakeDocumentWriteExceptionHandler catchesAllHandler = new FakeDocumentWriteExceptionHandler();
	private final FakeDocumentWriteExceptionHandler throwsHandler = new FakeDocumentWriteExceptionHandler(new TestException());
	private final ToggleableDocumentWriteExceptionHandler h = ToggleableDocumentWriteExceptionHandler.create(
			throwsHandler, catchesAllHandler);

	@Test
	public void defaultHandlerThrows() throws Exception {
		assertThrows(TestException.class, () -> h.handleException(new RuntimeException(), ""));
	}

	@Test
	public void doesNotThrowAfterSwitchToCatchesAllHandler() throws Exception {
		h.useCatchesAllHandler();
		h.handleException(new RuntimeException(), "");
	}

	@Test
	public void throwsAfterSwitchBackToDefaultHandler() throws Exception {
		h.useCatchesAllHandler();
		h.useDefaultHandler();
		assertThrows(TestException.class, () -> h.handleException(new RuntimeException(), ""));
	}

	public static class TestException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public TestException() {
		}

	}
}
