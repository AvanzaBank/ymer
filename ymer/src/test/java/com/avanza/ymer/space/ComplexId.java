package com.avanza.ymer.space;

import java.io.Serializable;
import java.util.Objects;

public class ComplexId implements Serializable {

	private String key1;
	private Integer key2;

	public ComplexId() {
	}

	public ComplexId(String key1, Integer key2) {
		this.key1 = key1;
		this.key2 = key2;
	}

	public String getKey1() {
		return key1;
	}

	public void setKey1(String key1) {
		this.key1 = key1;
	}

	public Integer getKey2() {
		return key2;
	}

	public void setKey2(Integer key2) {
		this.key2 = key2;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ComplexId complexId = (ComplexId) o;
		return Objects.equals(getKey1(), complexId.getKey1()) && Objects.equals(getKey2(), complexId.getKey2());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getKey1(), getKey2());
	}

	@Override
	public String toString() {
		return "ComplexId{" +
				"key1='" + key1 + '\'' +
				", key2=" + key2 +
				'}';
	}
}
