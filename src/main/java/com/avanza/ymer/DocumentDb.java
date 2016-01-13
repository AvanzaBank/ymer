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

import java.util.Objects;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.ReadPreference;
/**
 * 
 * @author Elias Lindholm (elilin)
 *
 */
final class DocumentDb {
	
	private final Provider provider;
	
	private DocumentDb(Provider provider) {
		this.provider = provider;
	}
	
	static DocumentDb create(Provider provider) {
		return new DocumentDb(provider);
	}
	
	static DocumentDb mongoDb(DB db, ReadPreference readPreference) {
		return new DocumentDb(new MongoDocumentDb(db, readPreference));
	}
	
	DocumentCollection getCollection(String name) {
		return provider.get(name);
	}
	
	interface Provider {
		DocumentCollection get(String name);
	}
	
	private static final class MongoDocumentDb implements DocumentDb.Provider {

		private final DB mongoDb;
		private final ReadPreference readPreference;
		
		MongoDocumentDb(DB mongoDb, ReadPreference readPreference) {
			this.readPreference = readPreference;
			this.mongoDb = Objects.requireNonNull(mongoDb);
		}

		@Override
		public DocumentCollection get(String name) {
			DBCollection collection = mongoDb.getCollection(name);
			collection.setReadPreference(readPreference);
			return new MongoDocumentCollection(collection);
		}
	}

}
