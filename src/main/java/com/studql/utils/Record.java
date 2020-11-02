package src.main.java.com.studql.utils;

import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;

public final class Record<T extends Boundable> {
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
