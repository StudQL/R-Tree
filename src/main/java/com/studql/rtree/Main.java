package src.main.java.com.studql.rtree;

import src.main.java.com.studql.shape.Rectangle;

public class Main {

	public static void main(String[] args) {
		Rectangle r1 = new Rectangle(1, 3, 4, 1);
		// should be 6.0
		System.out.println(r1.area());
		Rectangle r2 = new Rectangle(1, 2, 3, 1);
		// should be true
		System.out.println(r1.contains(r2));
		Rectangle r3 = new Rectangle(1, 4, 4, 1);
		// should be false
		System.out.println(r1.contains(r3));
	}

}
