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
