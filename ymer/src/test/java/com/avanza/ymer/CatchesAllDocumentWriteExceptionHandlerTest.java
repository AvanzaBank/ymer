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

import static org.junit.Assert.fail;

import org.junit.Test;

import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;

public class CatchesAllDocumentWriteExceptionHandlerTest {
	private final DocumentWriteExceptionHandler handler = new CatchesAllDocumentWriteExceptionHandler();

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

	private MongoSocketException newMongoNetworkException() {
		return new MongoSocketReadException("", new ServerAddress());
	}
}
