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

public interface BsonDocumentPatch {

	/**
	 * Applies this patch to the given document. <p>
	 *
	 * The patch should read the object for current state, and mutate it
	 * to reflect the patch.
	 *
	 */
	void apply(Document document);

	/**
	 * Returns the version that this patch applies to. A DocumentPatch only applies
	 * to a single version. A DocumentPatch is expected to patch the document
	 * to the next version. <p>
	 *
	 */
	int patchedVersion();
}
