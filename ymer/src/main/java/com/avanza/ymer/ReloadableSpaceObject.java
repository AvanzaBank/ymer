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

import com.gigaspaces.annotation.pojo.SpaceVersion;

/**
 * Denotes that this space object is capable of being reloaded by a {@link SpaceObjectLoader}. <p>
 * 
 * In order to be reloadable the space object must hold some meta-data about how it was last loaded from
 * the secondary storage. This is achieved by letting the getter methods in this interface return the last
 * value set by the corresponding setter. <p>
 * 
 * @author joasah Joakim Sahlström
 *
 */
public interface ReloadableSpaceObject {

	/**
	 * This should most likely be annotated with {@link SpaceVersion} in implementing classes
	 * @return current versionID
	 */
	public int getVersionID();

	/**
	 * Used by gigaspaces
	 * @param versionID
	 */
	public void setVersionID(int versionID);

	/**
	 * Used by mirror to determine what gs objects should be written to persistent storage
	 * @return {@link Integer} may be <code>null</code>
	 */
	public Integer getLatestRestoreVersion();

	/**
	 * Used by datasources to set at what version an {@link ReloadableSpaceObject} has be restored from persistent storage
	 * @param latestRestoreVersion
	 */
	public void setLatestRestoreVersion(Integer latestRestoreVersion);

}
