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

import java.time.Duration;

public final class PersistInstanceIdDefinition<T> {

	private static final Duration DEFAULT_DELAY = Duration.ofHours(1);

	private final MirroredObjectDefinition<T> parent;
	private boolean enabled;
	private boolean triggerCalculationOnStartup;
	private Duration triggerCalculationWithDelay = DEFAULT_DELAY;

	PersistInstanceIdDefinition(MirroredObjectDefinition<T> parent) {
		this.parent = parent;
	}

	static <T> PersistInstanceIdDefinition<T> from(PersistInstanceIdDefinition<T> from) {
		return new PersistInstanceIdDefinition<>(from.getParent())
				.enabled(from.enabled)
				.triggerCalculationOnStartup(from.triggerCalculationOnStartup)
				.triggerCalculationWithDelay(from.triggerCalculationWithDelay);
	}

	public PersistInstanceIdDefinition<T> enableWithDefaults() {
		return enabled(true).triggerCalculationOnStartup(true);
	}

	/**
	 * Whether to enable persisting the instance id for each document.
	 */
	public PersistInstanceIdDefinition<T> enabled(boolean enabled) {
		this.enabled = enabled;
		return this;
	}

	/**
	 * Whether to calculate persisted instance id on startup if necessary.
	 * When enabled, instance id will be calculated and persisted on startup (with a delay configured by
	 * {@code triggerCalculationWithDelay}).
	 * When disabled, {@link PersistedInstanceIdCalculationService} will only be run when manually called.
	 * This defaults to {@code true} when persisting instance id is enabled.
	 *
	 * Automatically starting this job requires {@link YmerSpaceSynchronizationEndpoint} to be handled
	 * as a Spring bean.
	 */
	public PersistInstanceIdDefinition<T> triggerCalculationOnStartup(boolean triggerCalculationOnStartup) {
		this.triggerCalculationOnStartup = triggerCalculationOnStartup;
		return this;
	}

	/**
	 * Delay triggering of calculation of persisted instance id with the specified delay after startup.
	 * This is done to reduce load on database during initial load of data.
	 * This property only has an effect when {@code triggerCalculationOnStartup} is enabled.
	 */
	public PersistInstanceIdDefinition<T> triggerCalculationWithDelay(Duration triggerCalculationWithDelay) {
		this.triggerCalculationWithDelay = triggerCalculationWithDelay;
		return this;
	}

	/**
	 * Return the {@code MirroredObjectDefinition} when done configuring the {@code PersistInstanceIdDefinition}.
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

	boolean isTriggerCalculationOnStartup() {
		return triggerCalculationOnStartup;
	}

	Duration getTriggerCalculationWithDelay() {
		return triggerCalculationWithDelay;
	}
}
