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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Holds information about all documents that are mirrored by a given VersionedMongoDBExternalDataSource. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 */
final class MirroredObjects {

	private final Map<Class<?>, MirroredObject<?>> mirroredObjectByType = new ConcurrentHashMap<>();
	
	/**
	 * Creates MirroredObjects holding the given documents. <p>
	 * 
	 * @param mirroredObjects
	 */
	MirroredObjects(MirroredObject<?>... mirroredObjects) {
		Stream.of(mirroredObjects).forEach(mirroredObject -> {
			this.mirroredObjectByType.put(mirroredObject.getMirroredType(), mirroredObject);
		});
	}
	
	MirroredObjects(Stream<MirroredObjectDefinition<?>> mirroredObjects) {
		mirroredObjects.map(MirroredObjectDefinition::buildMirroredDocument).forEach(mirroredObject -> {
			this.mirroredObjectByType.put(mirroredObject.getMirroredType(), mirroredObject);
		});
	}
	
	/**
	 * Returns a set of all mirrored types <p>
	 * 
	 * @return
	 */
	public Set<Class<?>> getMirroredTypes() {
		return mirroredObjectByType.keySet();
	}
	
	/**
	 * Returns a Collection of all MirroredObject's
	 * @return
	 */
	public Collection<MirroredObject<?>> getMirroredDocuments() {
		return mirroredObjectByType.values();
	}
	
	/**
	 * Returns the fully qualified classname for all mirrored types. <p>
	 * 
	 * @return
	 */
	public Set<String> getMirroredTypeNames() {
		Set<String> result = new HashSet<>();
		for (MirroredObject<?> doc : mirroredObjectByType.values()) {
			result.add(doc.getMirroredType().getName());
		}
		return result;
	}

	/**
	 * Returns the MirroredObject for a given type. <p>
	 * 
	 * @param type
	 * @return
	 * @throws NonMirroredTypeException if the given type is not mirrored
	 */
	@SuppressWarnings("unchecked")
	public <T> MirroredObject<T> getMirroredDocument(Class<T> type) {
		MirroredObject<T> result = (MirroredObject<T>) mirroredObjectByType.get(type);
		if (result == null) {
			throw new NonMirroredTypeException(type);
		}
		return result;
	}

	public boolean isMirroredType(Class<?> type) {
		return getMirroredTypes().contains(type);
	}
	
}
