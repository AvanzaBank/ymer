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
package com.avanza.ymer.mirror;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;

public class DocumentPatcherImpl<T> implements DocumentPatcher<T> {

	private static final Logger log = LoggerFactory.getLogger(DocumentPatcherImpl.class);

	private final MirroredDocument<T> document;
	private final DocumentConverter documentConverter;
	private final SpaceObjectFilter<T> spaceObjectFilter;

	public DocumentPatcherImpl(MirroredDocument<T> document, DocumentConverter documentConverter, SpaceObjectFilter<T> spaceObjectFilter) {
		this.document = Objects.requireNonNull(document);
		this.documentConverter = Objects.requireNonNull(documentConverter);
		this.spaceObjectFilter = Objects.requireNonNull(spaceObjectFilter);
	}
	
	@Override
	public Optional<T> patchAndConvert(BasicDBObject dbObject) {
		if (document.requiresPatching(dbObject)) {
			try {
				dbObject = document.patch(dbObject);
			} catch (RuntimeException e) {
				log.error("Patch of document failed! document=" + document + "dbObject=" + dbObject, e);
				throw e;
			}
		}
		T mirroredObject = documentConverter.convert(document.getMirroredType(), dbObject);
		if (!spaceObjectFilter.accept(mirroredObject)) {
			return Optional.empty();
		}
		return Optional.of(mirroredObject);
	}
	
	@Override
	public String getCollectionName() {
		return document.getCollectionName();
	}
}
