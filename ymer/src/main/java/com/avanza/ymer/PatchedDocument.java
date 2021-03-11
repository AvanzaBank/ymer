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

import org.bson.Document;

/**
 * Data structure to hold a patched document. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 */
final class PatchedDocument {
	private final Document oldVersion;
	private final Document newVersion;

	public PatchedDocument(Document oldVersion,
                             Document newVersion) {
		this.oldVersion = Objects.requireNonNull(oldVersion);
		this.newVersion = Objects.requireNonNull(newVersion);
	}
	
	/**
	 * The current version of the document in the database. <p>
	 * @return
	 */
	Document getOldVersion() {
		return oldVersion;
	}
	
	/**
	 * The new patched version that of the given document. <p>
	 * 
	 * @return
	 */
	Document getNewVersion() {
		return newVersion;
	}
	
}
