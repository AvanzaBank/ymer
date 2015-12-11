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

import com.avanza.gs.mongo.util.Require;
import com.mongodb.BasicDBObject;
/**
 * Data structure to hold a patched document. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 */
final class PatchedDocument {
	private final BasicDBObject oldVersion;
	private final BasicDBObject newVersion;

	public PatchedDocument(BasicDBObject oldVersion,
						   BasicDBObject newVersion) {
		this.oldVersion = Require.notNull(oldVersion);
		this.newVersion = Require.notNull(newVersion);
	}
	
	/**
	 * The current version of the document in the database. <p>
	 * @return
	 */
	BasicDBObject getOldVersion() {
		return oldVersion;
	}
	
	/**
	 * The new patched version that of the given document. <p>
	 * 
	 * @return
	 */
	BasicDBObject getNewVersion() {
		return newVersion;
	}
	
}