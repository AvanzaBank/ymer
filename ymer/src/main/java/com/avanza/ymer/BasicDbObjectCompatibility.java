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

import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Converts fields that have data types such as {@code List} or {@code Map} to
 * their corresponding {@code DBObject} datatype, to enable compatibility with
 * old document patches that have not migrated yet from {@code DocumentPatch} to
 * {@code BsonDocumentPatch}.
 */
@SuppressWarnings({ "rawtypes" })
class BasicDbObjectCompatibility {
	static BasicDBObject convertToBasicDBObject(Map map) {
		BasicDBObject dbObject = new BasicDBObject(map);
		dbObject.replaceAll((k, v) -> convertToDBObject(v));
		return dbObject;
	}

	@SuppressWarnings("unchecked")
	private static Object convertToDBObject(Object o) {
		if (o instanceof Map) {
			return convertToBasicDBObject((Map) o);
		} else if (o instanceof List) {
			BasicDBList list = new BasicDBList();
			((List) o).forEach(i -> list.add(convertToDBObject(i)));
			return list;
		}
		return o;
	}
}
