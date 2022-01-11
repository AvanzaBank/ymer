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
package com.avanza.ymer.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;

import com.avanza.ymer.TestSpaceObject;

public class GigaSpacesInstanceIdUtilTest {

	@Test
	public void shouldCalculateInstanceId() {
		TestSpaceObject spaceObject = new TestSpaceObject("testId", "testMessage");
		Object routingKey = spaceObject.getId();

		int instanceId_1_partitions = GigaSpacesInstanceIdUtil.getInstanceId(routingKey, 1);
		int instanceId_4_partitions = GigaSpacesInstanceIdUtil.getInstanceId(routingKey, 4);
		int instanceId_6_partitions = GigaSpacesInstanceIdUtil.getInstanceId(routingKey, 6);

		assertThat(instanceId_1_partitions, equalTo(1));
		assertThat(instanceId_4_partitions, equalTo(4));
		assertThat(instanceId_6_partitions, equalTo(2));
	}

 }
