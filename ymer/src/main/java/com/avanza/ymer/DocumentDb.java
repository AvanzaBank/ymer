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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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
	
	static DocumentDb mongoDb(MongoDatabase db, ReadPreference readPreference) {
		return new DocumentDb(new MongoDocumentDb(db, readPreference));
	}
	
	DocumentCollection getCollection(String name) {
		return getCollection(name, null);
	}

	DocumentCollection getCollection(String name, ReadPreference readPreference) {
		return provider.get(name, readPreference);
	}
	
	interface Provider {
		DocumentCollection get(String name, ReadPreference readPreference);
	}
	
	private static final class MongoDocumentDb implements DocumentDb.Provider {
		private static final Set<WriteConcern> EXPECTED_WRITE_CONCERNS = new HashSet<>(List.of(WriteConcern.ACKNOWLEDGED));
		private static final Logger LOGGER = LoggerFactory.getLogger(MongoDocumentDb.class);

		private final MongoDatabase mongoDatabase;
		private final ReadPreference readPreference;

		MongoDocumentDb(MongoDatabase mongoDb, ReadPreference readPreference) {
			this.readPreference = readPreference;
			this.mongoDatabase = Objects.requireNonNull(mongoDb);

			if (!EXPECTED_WRITE_CONCERNS.contains(mongoDb.getWriteConcern())) {
				LOGGER.error("Expected WriteConcern={} but was {}! Ymer is not designed for use with this WriteConcern and using it in production can/will lead to irrevocable data loss!", EXPECTED_WRITE_CONCERNS, mongoDb.getWriteConcern());
			}
		}

		@Override
		public DocumentCollection get(String name, ReadPreference readPreference) {
			MongoCollection<Document> collection = mongoDatabase.getCollection(name);
			collection.withReadPreference(Optional.ofNullable(readPreference)
												  .orElse(this.readPreference));
			return new MongoDocumentCollection(collection);
		}
	}

}
