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
