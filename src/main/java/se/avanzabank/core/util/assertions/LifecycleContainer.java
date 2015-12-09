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
package se.avanzabank.core.util.assertions;

import static java.util.Objects.requireNonNull;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for keeping lifecycle aware objects.
 * <p>
 * This is useful when having objects that is not exposed as Spring beans.
 * A common case is when you create objects in a factory.
 * 
 * @author Roger Forsberg
 *
 */
public final class LifecycleContainer {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private final List<LifecycleAware> objects = new LinkedList<>();
	
	/**
	 * Adds a lifecycle aware object to the container
	 */
	public final void add(LifecycleAware obj) {
		objects.add(requireNonNull(obj));
	}
	
	/**
	 * Calls destroy on all managed objects
	 */
	public final void destroyAll() {
		for (LifecycleAware obj: objects) {
			destroy(obj);
		}
	}

	private void destroy(LifecycleAware obj) {
		try {
			obj.destroy();
		} catch (Exception e) {
			logger.warn("Exception when destroying lifecycle aware object", e);
		}
	}
}
