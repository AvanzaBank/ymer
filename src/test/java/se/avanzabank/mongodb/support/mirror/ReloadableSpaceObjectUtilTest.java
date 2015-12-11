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

import static org.junit.Assert.*;

import org.junit.Test;



public class ReloadableSpaceObjectUtilTest {

	ReloadableSpaceObject rso = new ReloadableSpaceObject() {
		private int versionID;
		private Integer latestRestoreVersion;

		@Override
		public void setVersionID(int versionID) {
			this.versionID = versionID;
		}

		@Override
		public void setLatestRestoreVersion(Integer latestRestoreVersion) {
			this.latestRestoreVersion = latestRestoreVersion;
		}

		@Override
		public int getVersionID() {
			return versionID;
		}

		@Override
		public Integer getLatestRestoreVersion() {
			return latestRestoreVersion;
		}
	};

	@Test
	public void markingSetsBothVersionIDAndLatestRestoreVersion() throws Exception {
		ReloadableSpaceObjectUtil.markReloaded(rso);
		assertEquals(1, (int) rso.getLatestRestoreVersion());
		assertEquals(1, rso.getVersionID());
	}

	@Test
	public void correctlyIdentifiesMarkedObjects() throws Exception {
		assertFalse(ReloadableSpaceObjectUtil.isReloaded(rso));
		ReloadableSpaceObjectUtil.markReloaded(rso);
		assertTrue(ReloadableSpaceObjectUtil.isReloaded(rso));
	}

}
