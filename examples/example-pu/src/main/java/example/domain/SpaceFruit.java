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
package example.domain;

import org.springframework.data.annotation.Id;

import com.gigaspaces.annotation.pojo.SpaceId;

public class SpaceFruit {

	@Id
	private String name;
	private String origin;
	private boolean organic;
	
	public SpaceFruit() {
	}
	
	public SpaceFruit(String name, String origin, boolean organic) {
		this.name = name;
		this.origin = origin;
		this.organic = organic;
	}

	@SpaceId(autoGenerate=false)
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setOrigin(String origin) {
		this.origin = origin;
	}
	
	public String getOrigin() {
		return origin;
	}
	
	public boolean isOrganic() {
		return organic;
	}
	
	public void setOrganic(boolean organic) {
		this.organic = organic;
	}

}
