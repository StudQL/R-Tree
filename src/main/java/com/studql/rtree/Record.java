package src.main.java.com.studql.rtree;

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
}
