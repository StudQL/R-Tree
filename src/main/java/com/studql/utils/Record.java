package com.studql.utils;

import java.io.Serializable;

import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;

public class Record<T extends Boundable> implements Serializable {
	private static final long serialVersionUID = -8270017566173656873L;
	private final String identifier;
	private final T value;

	public Record(T value, String identifier) {
		this.value = value;
		this.identifier = identifier;
	}

	public T getValue() {
		return this.value;
	}

	public String getIdentifier() {
		return this.identifier;
	}

	public Rectangle getMbr() {
		return this.value.getMbr();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		Record<T> r = (Record<T>) obj;
		return r.getValue().equals(this.value);
	}

	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Record(");
		if (this.identifier != null) {
			strBuilder.append("ID=" + this.identifier);
		}
		strBuilder.append(", value=" + this.value.toString() + ")");
		return strBuilder.toString();
	}

}
