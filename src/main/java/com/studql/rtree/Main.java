package src.main.java.com.studql.rtree;

import java.util.ArrayList;

import src.main.java.com.studql.shape.Point;
import src.main.java.com.studql.shape.Rectangle;

public class Main {

	public static void test_rectangles() {
		int min_records = 1;
		int max_records = 2;
		Rtree<Rectangle> tree = new Rtree<Rectangle>(min_records, max_records,
				new QuadraticSplitter<Rectangle>(min_records));

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
		Rectangle rec = new Rectangle(1, 14, 4, 5);
		Record<Rectangle> rec2 = new Record<Rectangle>(new Rectangle(12, 13, 1, 2), "");
		ArrayList<Record<Rectangle>> testo = tree.rangeSearch(tree.getRoot(), rec);
		ArrayList<Record<Rectangle>> rec3 = tree.search(tree.getRoot(), rec2);
		System.out.println("Rectangles:" + testo);
		System.out.println("Rectangle:" + rec3);
		System.out.println("taille : " + tree.calculateHeight());


	}

	public static void test_points() {
		int min_records = 2;
		int max_records = 4;
		Rtree<Point> tree = new Rtree<Point>(min_records, max_records, new QuadraticSplitter<Point>(min_records));

		@SuppressWarnings("serial")
		ArrayList<Record<Point>> dataPoints = new ArrayList<Record<Point>>() {
			{
				add(new Record<Point>(new Point(1, 2), "1"));
				add(new Record<Point>(new Point(3, 4), "2"));
				add(new Record<Point>(new Point(5, 2), "3"));
				add(new Record<Point>(new Point(6, 3), "4"));
				add(new Record<Point>(new Point(7, 5), "5"));
				add(new Record<Point>(new Point(8, 4), "6"));
				add(new Record<Point>(new Point(9, 2), "7"));
				add(new Record<Point>(new Point(10, 5), "8"));
				add(new Record<Point>(new Point(11, 3), "9"));
				add(new Record<Point>(new Point(12, 1), "10"));
				add(new Record<Point>(new Point(13, 2), "11"));
				add(new Record<Point>(new Point(4, 3), "12"));
			}
		};
		for (Record<Point> r : dataPoints) {
			tree.insert(r);
		}
		System.out.println(tree.toString());
		Rectangle rec = new Rectangle(1, 14, 4, 5);
		Record<Point> pt = new Record<Point>(new Point(4, 3), "");
		ArrayList<Record<Point>> testo = tree.rangeSearch(tree.getRoot(), rec);
		ArrayList<Record<Point>> point = tree.search(tree.getRoot(), pt);
		System.out.println("Points:" + testo);
		System.out.println("Point:" + point);
		System.out.println("taille : " + tree.calculateHeight());
	}

	public static void main(String[] args) {
		//test_rectangles();
		test_points();
		
	}

}
