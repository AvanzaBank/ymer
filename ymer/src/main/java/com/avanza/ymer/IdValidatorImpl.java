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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

class IdValidatorImpl implements MongoDocumentCollection.IdValidator {
	private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentCollection.class);
	private static final String ID_FIELD = "_id";
	private final String collectionName;

	IdValidatorImpl(String collectionName) {
		this.collectionName = collectionName;
	}

	@Override
	public void validateHasIdField(String operation, Document obj) {
		if (obj.get(ID_FIELD) == null) {
			warnAboutMissingIdField(operation);
		}
	}

	void warnAboutMissingIdField(String operation) {
		LOG.warn("Will {} a document on collection={} without id! "
						+ "This is almost always an error. "
						+ "Is the @Id field missing for objects of this type?",
				operation,
				collectionName
		);
	}

	@Override
	public void validateUpdatedExistingDocument(
			String operation,
			UpdateResult result,
			Document obj
	) {
		if (!result.wasAcknowledged()) {
			// No way to validate when using WriteConcern.UNACKNOWLEDGED
			return;
		}
		if (didNotMatchAnyDocuments(result)) {
			warnAboutNoDocumentMatch(operation, obj.get(ID_FIELD));
		}
	}

	private boolean didNotMatchAnyDocuments(UpdateResult result) {
		return result.getMatchedCount() == 0;
	}

	@Override
	public void validateDeletedExistingDocument(
			String operation,
			DeleteResult result,
			Document obj
	) {
		if (!result.wasAcknowledged()) {
			// No way to validate when using WriteConcern.UNACKNOWLEDGED
			return;
		}
		if (didNotMatchAnyDocuments(result)) {
			warnAboutNoDocumentMatch(operation, obj.get(ID_FIELD));
		}
	}

	private boolean didNotMatchAnyDocuments(DeleteResult result) {
		return result.getDeletedCount() == 0;
	}

	void warnAboutNoDocumentMatch(String operation, Object id) {
		LOG.warn("Tried to {} a document on collection={} with id={} , and no such document was found! "
						+ "Is the @Id field missing for objects of this type?",
				operation,
				collectionName,
				id
		);
	}
}
