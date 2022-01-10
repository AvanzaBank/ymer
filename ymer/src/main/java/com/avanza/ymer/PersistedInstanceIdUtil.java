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

import static com.avanza.ymer.MirroredObject.DOCUMENT_INSTANCE_ID_PREFIX;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.springframework.data.mongodb.core.index.IndexInfo;

final class PersistedInstanceIdUtil {

	private PersistedInstanceIdUtil() {
	}

	public static Predicate<IndexInfo> isIndexForAnyNumberOfPartitionsIn(Set<Integer> numberOfPartitions) {
		return indexInfo -> numberOfPartitions.stream().anyMatch(fieldName -> isIndexForNumberOfPartitions(fieldName).test(indexInfo));
	}

	public static Predicate<IndexInfo> isIndexForNumberOfPartitions(int numberOfPartitions) {
		final String fieldName = getInstanceIdFieldName(numberOfPartitions);
		return indexInfo -> indexInfo.isIndexForFields(List.of(fieldName));
	}

	public static String getInstanceIdFieldName(int numberOfPartitions) {
		return DOCUMENT_INSTANCE_ID_PREFIX + "_" + numberOfPartitions;
	}
}
