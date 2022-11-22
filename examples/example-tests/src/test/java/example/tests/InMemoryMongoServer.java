package example.tests;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

final class InMemoryMongoServer {

	private static final Logger LOG = LoggerFactory.getLogger(InMemoryMongoServer.class);

	static final String DEFAULT_MONGODB_HOST = "localhost";
	static final int DEFAULT_MONGODB_PORT = 27017;

	private InMemoryMongoServer() {}

	public static MongoServer getInstance() {
		return getInstance(DEFAULT_MONGODB_HOST, DEFAULT_MONGODB_PORT);
	}

	public static MongoServer getInstance(final int port) {
		return getInstance(DEFAULT_MONGODB_HOST, port);
	}

	private static MongoServer getInstance(final String host, final int port) {
		MongoServer mongoServer = new MongoServer(new MemoryBackend());
		LOG.info("Setting up MongoServer with host={} port={}...", host, port);
		mongoServer.bind(host, port);
		return mongoServer;
	}
}
