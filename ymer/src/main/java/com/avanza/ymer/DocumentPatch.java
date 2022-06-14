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

import static com.avanza.ymer.BasicDbObjectCompatibility.convertToBasicDBObject;

import org.bson.Document;

import com.mongodb.BasicDBObject;
/**
 * A DocumentPatch patches a given document from one version to the next.
 * 
 * @author Elias Lindholm (elilin)
 * @deprecated use {@link BsonDocumentPatch}
 */
@Deprecated(since = "3.0.5")
public interface DocumentPatch extends BsonDocumentPatch {
	
	/**
	 * Applies this patch to the given document. <p>
	 * 
	 * The patch should read the object for current state, and mutate it 
	 * to reflect the patch. 
	 *
	 */
	void apply(BasicDBObject dbObject);

	@Override
	default void apply(Document document) {
		BasicDBObject dbo = convertToBasicDBObject(document);
		apply(dbo);
		document.putAll(dbo); // Ensures that new properties are added and replaced properties are updated.
		document.keySet().retainAll(dbo.keySet()); // Ensures that removed properties are deleted.
	}

	/**
	 * Returns the version that this patch applies to. A DocumentPatch only applies 
	 * to a single version. A DocumentPatch is expected to patch the document
	 * to the next version.
	 * This patch will be applied to all documents whose version is equal or
	 * lower than this number. After this patch has been applied, the document
	 * will have its version updated to {@code patchedVersion + 1}. Documents
	 * that have not had any patches applied, or otherwise are missing the
	 * version field, will be considered to be at version {@code 1}. Therefore,
	 * if this is the first patch to apply for a particular document type, set
	 * this value to {@code 1}.
	 */
	@Override
	int patchedVersion();
}
