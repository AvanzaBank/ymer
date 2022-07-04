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

import java.util.Objects;

import org.springframework.data.annotation.Id;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

public class TestSpaceObjectWithCustomRoutingKey {

	@Id
	private String id;
	private String customRoutingKey;

	public TestSpaceObjectWithCustomRoutingKey(String id, String customRoutingKey) {
		this.id = id;
		this.customRoutingKey = customRoutingKey;
	}

	public TestSpaceObjectWithCustomRoutingKey() {
	}

	@SpaceId
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@SpaceRouting
	public String getCustomRoutingKey() {
		return customRoutingKey;
	}

	public void setCustomRoutingKey(String customRoutingKey) {
		this.customRoutingKey = customRoutingKey;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TestSpaceObjectWithCustomRoutingKey that = (TestSpaceObjectWithCustomRoutingKey) o;
		return id.equals(that.id) && customRoutingKey.equals(that.customRoutingKey);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, customRoutingKey);
	}

	@Override
	public String toString() {
		return "TestSpaceObjectWithCustomRoutingKey [id=" + id + ", customRoutingKey=" + customRoutingKey + "]";
	}
}
