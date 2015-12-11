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
package com.avanza.gs.mongo.mirror;

/**
 * Exception indicating a transient problem with writing an object to the data source. Typically network related
 * problems.
 * 
 * @author Kristoffer Erlandsson (krierl), kristoffer.erlandsson@avanzabank.se
 */
public class DocumentWriteTransientException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public DocumentWriteTransientException(String message, Throwable cause) {
		super(message, cause);
	}

	public DocumentWriteTransientException(Throwable cause) {
		super(cause);
	}
}
