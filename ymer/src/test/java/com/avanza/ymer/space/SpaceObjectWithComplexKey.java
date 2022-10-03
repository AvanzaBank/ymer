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
package com.avanza.ymer.space;

import java.util.Objects;

import org.springframework.data.annotation.Id;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

@SpaceClass
public class SpaceObjectWithComplexKey {

	@Id
	private ComplexId id;
	private String message;

	public SpaceObjectWithComplexKey() {
	}

	public SpaceObjectWithComplexKey(ComplexId id, String message) {
		this.id = id;
		this.message = message;
	}

	@SpaceId
	@SpaceRouting
	public ComplexId getId() {
		return id;
	}

	public void setId(ComplexId id) {
		this.id = id;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SpaceObjectWithComplexKey that = (SpaceObjectWithComplexKey) o;
		return Objects.equals(getId(), that.getId()) && Objects.equals(getMessage(), that.getMessage());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId(), getMessage());
	}

	@Override
	public String toString() {
		return "SpaceObjectWithComplexKey{" +
				"id=" + id +
				", message='" + message + '\'' +
				'}';
	}
}
