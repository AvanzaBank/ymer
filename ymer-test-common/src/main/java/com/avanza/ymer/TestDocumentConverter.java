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

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;

/**
 * Makes document conversion through {@link DocumentConverter} available in tests.
 */
public final class TestDocumentConverter {

	private final DocumentConverter documentConverter;

	private TestDocumentConverter(DocumentConverter documentConverter) {
		this.documentConverter = documentConverter;
	}

	public static TestDocumentConverter create(MongoConverter mongoConverter) {
		return new TestDocumentConverter(DocumentConverter.mongoConverter(mongoConverter));
	}

	public Document convertToBsonDocument(Object type) {
		return documentConverter.convertToBsonDocument(type);
	}

	public <T> T convert(Class<T> toType, Document document) {
		return documentConverter.convert(toType, document);
	}

}
