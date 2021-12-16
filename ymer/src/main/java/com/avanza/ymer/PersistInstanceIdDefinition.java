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
/*
o * Copyright 2015 Avanza Bank AB
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

public final class PersistInstanceIdDefinition<T> {

	private static final int DEFAULT_DELAY = 60 * 60;

	private final MirroredObjectDefinition<T> parent;
	private boolean enabled;
	private boolean recalculateOnStartup;
	private int recalculateWithDelay = DEFAULT_DELAY;

	PersistInstanceIdDefinition(MirroredObjectDefinition<T> parent) {
		this.parent = parent;
	}

	static <T> PersistInstanceIdDefinition<T> from(PersistInstanceIdDefinition<T> from) {
		return new PersistInstanceIdDefinition<>(from.getParent())
				.enabled(from.enabled)
				.recalculateOnStartup(from.recalculateOnStartup)
				.recalculateWithDelay(from.recalculateWithDelay);
	}

	public PersistInstanceIdDefinition<T> enableWithDefaults() {
		return enabled(true).recalculateOnStartup(true);
	}

	/**
	 * Whether to enable persisting the instance id for each document.
	 */
	public PersistInstanceIdDefinition<T> enabled(boolean enabled) {
		this.enabled = enabled;
		return this;
	}

	/**
	 * Whether to recalculate persisted instance id on startup if necessary.
	 * If enabled, instance id will be recalculated on startup (with a delay configured by
	 * {@code delayRecalculationOnStartupWithSeconds}).
	 * If disabled, instance id will only be recalculated when manually called.
	 * This defaults to {@code true} when persisting instance id is enabled.
	 *
	 * Automatically starting this job requires {@link YmerSpaceSynchronizationEndpoint} to be handled
	 * as a Spring bean.
	 */
	public PersistInstanceIdDefinition<T> recalculateOnStartup(boolean recalculateOnStartup) {
		this.recalculateOnStartup = recalculateOnStartup;
		return this;
	}

	/**
	 * Delay starting recalculation of persisted instance id with the specified delay after startup (in seconds).
	 * This is done do reduce load on database during initial load of data.
	 */
	public PersistInstanceIdDefinition<T> recalculateWithDelay(int recalculateOnStartupDelayInSeconds) {
		this.recalculateWithDelay = recalculateOnStartupDelayInSeconds;
		return this;
	}

	/**
	 * Return the MirroredObjectDefinition when done configuring the PersistInstanceIdDefinition.
	 * This is useful for method chaining.
	 *
	 * @return the MirroredObjectDefinition for further customizations
	 */
	public MirroredObjectDefinition<T> and() {
		return getParent();
	}

	private MirroredObjectDefinition<T> getParent() {
		return parent;
	}

	boolean isEnabled() {
		return enabled;
	}

	boolean isRecalculateOnStartup() {
		return recalculateOnStartup;
	}

	int getRecalculateWithDelay() {
		return recalculateWithDelay;
	}
}
