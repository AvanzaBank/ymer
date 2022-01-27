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

public final class PersistInstanceIdDefinition {

	private static final Duration DEFAULT_DELAY = Duration.ofHours(1);

	private boolean enabled = false;
	private boolean triggerCalculationOnStartup = true;
	private Duration triggerCalculationWithDelay = DEFAULT_DELAY;

	static PersistInstanceIdDefinition from(PersistInstanceIdDefinition from) {
		return new PersistInstanceIdDefinition()
				.enabled(from.enabled)
				.triggerCalculationOnStartup(from.triggerCalculationOnStartup)
				.triggerCalculationWithDelay(from.triggerCalculationWithDelay);
	}

	/**
	 * Whether to enable persisting the instance id for each document.
	 */
	public PersistInstanceIdDefinition enabled(boolean enabled) {
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
	public PersistInstanceIdDefinition triggerCalculationOnStartup(boolean triggerCalculationOnStartup) {
		this.triggerCalculationOnStartup = triggerCalculationOnStartup;
		return this;
	}

	/**
	 * Delay triggering of calculation of persisted instance id with the specified delay after startup.
	 * This is done to reduce load on database during initial load of data.
	 * This property only has an effect when {@code triggerCalculationOnStartup} is enabled.
	 */
	public PersistInstanceIdDefinition triggerCalculationWithDelay(Duration triggerCalculationWithDelay) {
		this.triggerCalculationWithDelay = triggerCalculationWithDelay;
		return this;
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
