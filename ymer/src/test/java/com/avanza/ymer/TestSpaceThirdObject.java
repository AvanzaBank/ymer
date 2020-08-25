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

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.mongodb.BasicDBObject;

public class TestSpaceThirdObject {

	public static class TestSpaceThirdObjectPatchV1 implements DocumentPatch {
		@Override
		public void apply(BasicDBObject dbObject) {
			String name = (String) dbObject.get("name");
			dbObject.put("name", "b" + name);
		}

		@Override
		public int patchedVersion() {
			return 1;
		}
	}

	private String id;
	private String name;

	public TestSpaceThirdObject() {
		// for gigaspaces
	}

	public TestSpaceThirdObject(String id, String name) {
		this.id = id;
		this.name = name;
	}

	@SpaceId
	@SpaceRouting
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		TestSpaceThirdObject other = (TestSpaceThirdObject) obj;
		return this.id.equals(other.id)
				&& this.name.equals(other.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, name);
	}

	@Override
	public String toString() {
		return "TestSpaceThirdObject [id=" + id + ", name=" + name + "]";
	}

}
