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
package se.avanzabank.mongodb.support;

import java.util.Objects;

/**
 * Exception handler that can toggle between a default exception handler and one that catches all exceptions.
 * 
 * @author Kristoffer Erlandsson (krierl), kristoffer.erlandsson@avanzabank.se
 */
public class ToggleableDocumentWriteExceptionHandler implements DocumentWriteExceptionHandler,
		ToggleableDocumentWriteExceptionHandlerMBean {

	private final DocumentWriteExceptionHandler defaultHandler;
	private final DocumentWriteExceptionHandler catchesAllExceptionsHandler;
	private volatile DocumentWriteExceptionHandler state;

	private ToggleableDocumentWriteExceptionHandler(DocumentWriteExceptionHandler defaultHandler,
			DocumentWriteExceptionHandler catchesAllExceptionsHandler) {
		this.catchesAllExceptionsHandler = Objects.requireNonNull(catchesAllExceptionsHandler);
		this.defaultHandler = Objects.requireNonNull(defaultHandler);
		state = defaultHandler;
	}

	public static ToggleableDocumentWriteExceptionHandler create(DocumentWriteExceptionHandler defaultHandler,
			DocumentWriteExceptionHandler catchesAllExceptionsHandler) {
		return new ToggleableDocumentWriteExceptionHandler(defaultHandler, catchesAllExceptionsHandler);
	}

	@Override
	public void handleException(Exception exception, String operationDescription) {
		state.handleException(exception, operationDescription);
	}

	public void useCatchesAllHandler() {
		state = catchesAllExceptionsHandler;
	}

	public void useDefaultHandler() {
		state = defaultHandler;
	}

}
