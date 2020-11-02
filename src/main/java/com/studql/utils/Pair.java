package src.main.java.com.studql.utils;

public class Pair<T, U> {
	private T val1;
	private U val2;

	public Pair(T val1, U val2) {
		this.val1 = val1;
		this.val2 = val2;
	}

	public T getFirst() {
		return this.val1;
	}

	public U getSecond() {
		return this.val2;
	}

	public String toString() {
		return "<" + val1.toString() + ", " + val2.toString() + ">";
	}
}
