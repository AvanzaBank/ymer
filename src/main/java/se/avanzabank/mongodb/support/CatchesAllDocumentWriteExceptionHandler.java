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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatchesAllDocumentWriteExceptionHandler implements DocumentWriteExceptionHandler {

	private Logger log = LoggerFactory.getLogger(CatchesAllDocumentWriteExceptionHandler.class);
	
	@Override
	public void handleException(Exception exception, String operationDescription) {
		log.error(
				"Exception when executing mirror command! Attempted operation: " + operationDescription
						+ " - This command will be ignored but the rest" +
						" of the commands in this bulk will be attempted." +
						" This can lead to data inconsistency in the mongo database." +
						" Must be investigated ASAP.",
				exception);
	}

}
