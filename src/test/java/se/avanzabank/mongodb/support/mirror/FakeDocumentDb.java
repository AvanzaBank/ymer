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
package se.avanzabank.mongodb.support.mirror;

import java.util.concurrent.ConcurrentHashMap;

public class FakeDocumentDb implements DocumentDb.Provider {

	private ConcurrentHashMap<String, FakeDocumentCollection> collectionByName = new ConcurrentHashMap<>();
	
	@Override
	public DocumentCollection get(String name) {
		FakeDocumentCollection documentCollection = new FakeDocumentCollection();
		collectionByName.putIfAbsent(name, documentCollection);
		return collectionByName.get(name);
	}

	public static DocumentDb create() {
		return DocumentDb.create(new FakeDocumentDb());
	}

}
