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

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.function.Supplier;

public final class ReloadableYmerProperties {

	private final Supplier<Optional<Integer>> nextNumberOfInstances;

	private ReloadableYmerProperties(Supplier<Optional<Integer>> nextNumberOfInstances) {
		this.nextNumberOfInstances = requireNonNull(nextNumberOfInstances);
	}

	public Optional<Integer> getNextNumberOfInstances() {
		return nextNumberOfInstances.get()
				.filter(numberOfInstances -> numberOfInstances > 0);
	}

	static ReloadablePropertiesBuilder builder() {
		return new ReloadablePropertiesBuilder();
	}

	public static final class ReloadablePropertiesBuilder {
		private Supplier<Optional<Integer>> nextNumberOfInstances = Optional::empty;

		private ReloadablePropertiesBuilder() {
		}

		/**
		 * Sets a supplier returning the coming amount of partitions planned for the space after restart.
		 * This is designed to be used in combination with {@link MirroredObjectDefinition#persistInstanceId()}.
		 * <p>
		 * When writing the instance ID field, both the instance ID using the current number of partitions and the next
		 * number of partitions will be set to the document. It will also be used when calling {@link PersistedInstanceIdRecalculationService}.
		 * <p>
		 * Only values from 1 and above are valid, other values will be ignored.
		 */
		public ReloadablePropertiesBuilder nextNumberOfInstances(Supplier<Optional<Integer>> nextNumberOfInstances) {
			this.nextNumberOfInstances = nextNumberOfInstances;
			return this;
		}

		public ReloadableYmerProperties build() {
			return new ReloadableYmerProperties(nextNumberOfInstances);
		}
	}
}
