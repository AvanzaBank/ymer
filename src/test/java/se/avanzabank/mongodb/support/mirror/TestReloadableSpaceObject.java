package se.avanzabank.mongodb.support.mirror;

import com.gigaspaces.annotation.pojo.SpaceId;

public class TestReloadableSpaceObject implements ReloadableSpaceObject {

	private int id;
	private boolean patched;
	// from ReloadableSpaceObject
	private int versionID;
	private Integer latestRestoreVersion;

	public TestReloadableSpaceObject(int id, int spaceRouting, boolean patched, int versionID, Integer latestRestoreVersion) {
		this.id = id;
		this.patched = patched;
		this.versionID = versionID;
		this.latestRestoreVersion = latestRestoreVersion;
	}

	public TestReloadableSpaceObject() {
	}

	public void setPatched(boolean patched) {
		this.patched = patched;
	}

	public boolean isPatched() {
		return patched;
	}

	@SpaceId
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return toString().equals(obj.toString());
	}

	@Override
	public String toString() {
		return "FakeSpaceObject [id=" + id + ", patched=" + patched + "]";
	}

	@Override
	public int getVersionID() {
		return versionID;
	}

	@Override
	public void setVersionID(int versionID) {
		this.versionID = versionID;
	}

	@Override
	public Integer getLatestRestoreVersion() {
		return latestRestoreVersion;
	}

	@Override
	public void setLatestRestoreVersion(Integer latestRestoreVersion) {
		this.latestRestoreVersion = latestRestoreVersion;
	}

}
