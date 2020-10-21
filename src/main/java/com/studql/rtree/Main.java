package src.main.java.com.studql.rtree;

import java.util.ArrayList;

import src.main.java.com.studql.shape.Rectangle;

public class Main {

	public static void main(String[] args) {
		int min_records = 1;
		int max_records = 2;
		Rtree<Rectangle> tree = new Rtree<Rectangle>(min_records, max_records);
		@SuppressWarnings("serial")
		ArrayList<Record<Rectangle>> dataPoints = new ArrayList<Record<Rectangle>>() {
			{
				add(new Record<Rectangle>(new Rectangle(1, 2, 2, 4), "1"));
				add(new Record<Rectangle>(new Rectangle(3, 4, 4, 5), "2"));
				add(new Record<Rectangle>(new Rectangle(6, 7, 2, 3), "3"));
				add(new Record<Rectangle>(new Rectangle(5, 6, 1, 2), "4"));
				add(new Record<Rectangle>(new Rectangle(8, 9, 2, 4), "5"));
				add(new Record<Rectangle>(new Rectangle(10, 11, 4, 5), "6"));
				add(new Record<Rectangle>(new Rectangle(13, 14, 2, 3), "7"));
				add(new Record<Rectangle>(new Rectangle(12, 13, 1, 2), "8"));
			}
		};
		for (Record<Rectangle> r : dataPoints) {
			tree.insert(r);
		}
		System.out.println(tree.toString());
	}

}
