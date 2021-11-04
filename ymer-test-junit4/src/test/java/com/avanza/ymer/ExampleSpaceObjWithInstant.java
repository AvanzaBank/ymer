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

import java.io.Serializable;
import java.time.Instant;

import org.springframework.data.annotation.Id;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

@SpaceClass
public class ExampleSpaceObjWithInstant implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	private String id;
	private Instant timestamp;

	public ExampleSpaceObjWithInstant() {/*Required by GigaSpaces*/}

	public ExampleSpaceObjWithInstant(String id, Instant timestamp) {
		this.id = id;
		this.timestamp = timestamp;
	}

	@SpaceId
	@SpaceRouting
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Instant timestamp) {
		this.timestamp = timestamp;
	}
}
