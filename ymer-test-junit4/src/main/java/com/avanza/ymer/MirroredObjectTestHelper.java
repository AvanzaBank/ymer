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

import java.util.Collection;

import org.bson.Document;

/**
 * Makes package-private methods needed for testing {@link MirroredObject} available in tests.
 */
final class MirroredObjectTestHelper {

	private final MirroredObjects mirroredObjects;
	private final Class<?> objectClass;

	private MirroredObjectTestHelper(MirroredObjects mirroredObjects, Class<?> objectClass) {
		this.mirroredObjects = mirroredObjects;
		this.objectClass = objectClass;
	}

	public static MirroredObjectTestHelper fromDefinitions(Collection<MirroredObjectDefinition<?>> definitions, Class<?> objectClass) {
		MirroredObjects mirroredObjects = new MirroredObjects(definitions.stream(), MirroredObjectDefinitionsOverride.noOverride());
		return new MirroredObjectTestHelper(mirroredObjects, objectClass);
	}

	public boolean isMirroredType() {
		return mirroredObjects.isMirroredType(objectClass);
	}

	public Class<?> getMirroredType() {
		return getMirroredObject().getMirroredType();
	}

	public void setDocumentVersion(Document document, int version) {
		getMirroredObject().setDocumentVersion(document, version);
	}

	public int getDocumentVersion(Document document) {
		return getMirroredObject().getDocumentVersion(document);
	}

	public void patchToNextVersion(Document document) {
		getMirroredObject().patchToNextVersion(document);
	}

	public boolean requiresPatching(Document document) {
		return getMirroredObject().requiresPatching(document);
	}

	private MirroredObject<?> getMirroredObject() {
		return mirroredObjects.getMirroredObject(objectClass);
	}
}
