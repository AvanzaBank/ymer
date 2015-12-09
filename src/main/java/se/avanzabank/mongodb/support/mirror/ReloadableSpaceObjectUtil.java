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
package se.avanzabank.mongodb.support.mirror;

/**
 * Used for handling ReloadableSpaceObject:s
 *
 * @author joasah Joakim Sahlström
 *
 */
/*package*/ class ReloadableSpaceObjectUtil {

	private ReloadableSpaceObjectUtil() {
		// hide me
	}

	/*package*/ static void markReloaded(ReloadableSpaceObject reloadableSpaceObject) {
		// GigaSpaces will set version id to 1 during insert operation into space, even if it's >1 in MongoDb.
		int versionId = 1;
		reloadableSpaceObject.setVersionID(versionId);
		reloadableSpaceObject.setLatestRestoreVersion(versionId);
	}

	/*package*/ static boolean isReloaded(ReloadableSpaceObject reloadableSpaceObject) {
		return reloadableSpaceObject.getLatestRestoreVersion() != null
				&& reloadableSpaceObject.getLatestRestoreVersion().intValue() == reloadableSpaceObject.getVersionID();
	}

}
