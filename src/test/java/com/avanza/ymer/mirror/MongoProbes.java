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


import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.springframework.data.mongodb.core.MongoOperations;

import com.avanza.ymer.test.util.Probe;

/**
 * Probes to assist with asynchronous testing with Mongo.
 * 
 * @author Kristoffer Erlandsson (krierl), kristoffer.erlandsson@avanzabank.se
 */
public class MongoProbes {

	/**
	 * This probe checks all objects of the specified class in the mongo instance. If any of them matches the matcher,
	 * the probe is satisfied.
	 */
	public static <T> Probe containsObject(final MongoOperations mongo, final Matcher<T> matcher,
			final Class<T> objectClass) {
		return new Probe() {

			private List<T> sampledObjects = new ArrayList<T>();

			@Override
			public void sample() {
				sampledObjects = mongo.findAll(objectClass);
			}

			@Override
			public boolean isSatisfied() {
				for (T object : sampledObjects) {
					if (matcher.matches(object)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public void describeFailureTo(Description description) {
				StringBuilder sb = new StringBuilder();
				for (T details : sampledObjects) {
					sb.append(details);
					sb.append(" ");
				}
				description.appendText("failed to find object in mongo that ").appendDescriptionOf(matcher)
						.appendText(" - most recent sampled objects: ").appendText(sb.toString().trim());
			}
		};

	}

}
