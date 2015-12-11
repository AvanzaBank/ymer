package se.avanzabank.mongodb.support.mirror;

import org.springframework.data.annotation.Id;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

public class TestSpaceOtherObject {

	@Id
	private String id;
	private String message;
	
	public TestSpaceOtherObject(String id, String message) {
		this.id = id;
		this.message = message;
	}
	
	public TestSpaceOtherObject() {
	}

	@SpaceId
	@SpaceRouting
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TestSpaceOtherObject other = (TestSpaceOtherObject) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TestSpaceOtherObject [id=" + id + ", message=" + message + "]";
	}
	
}
