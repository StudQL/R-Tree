package src.main.java.com.studql.rtree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import src.main.java.com.studql.shape.Point;
import src.main.java.com.studql.shape.Rectangle;
import src.main.java.com.studql.utils.Benchmark;
import src.main.java.com.studql.utils.Visualizer;

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
				add(new Record<Rectangle>(new Rectangle(13, 15, 2, 3), "7"));
				add(new Record<Rectangle>(new Rectangle(12, 13, 1, 2), "8"));
			}
		};
		for (Record<Rectangle> r : dataPoints) {
			tree.insert(r);
		}
		System.out.println(tree.toString());

		Visualizer<Rectangle> v = new Visualizer<Rectangle>();
		try {
			v.createVisualization(tree, new File("D:\\Users\\utilisateur\\Downloads\\test.png"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		for (Record<Rectangle> r : dataPoints) {
//			tree.delete(r);
//			System.out.println(tree.toString());
//		}
	}

	public static void test_points() {
		int min_records = 2;
		int max_records = 4;
		Rtree<Point> tree = new Rtree<Point>(min_records, max_records, new LinearSplitter<Point>(min_records));

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

		Visualizer<Point> v = new Visualizer<Point>();
		try {
			v.createVisualization(tree, new File("D:\\Users\\utilisateur\\Downloads\\test.png"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		for (Record<Point> r : dataPoints) {
//			tree.delete(r);
//			System.out.println(tree.toString());
//		}
	}

	public static void main(String[] args) {
		String fileLocation = "D:\\Users\\utilisateur\\Downloads\\";
		test_points();
		
//		test_rectangles();
		Benchmark b = new Benchmark(fileLocation);
		int n = 1000;
		int[] xRange = new int[] { 0, 400 };
		int[] yRange = new int[] { 0, 600 };
		int[] page_sizes = new int[] { 6, 12, 25, 50, 102 };
		List<Function<Integer, Integer>> min_page_operators = Arrays.asList(num -> Math.round(num) / 3,
				num -> Math.round(num) / 2, num -> 2);
		boolean shouldVisualize = true;

		b.benchmarkInsertWithRandomPoints(n, xRange, yRange, page_sizes, min_page_operators, shouldVisualize);
		//b.benchmarkInsertWithDatasetPoints(fileLocation, page_sizes, min_page_operators, shouldVisualize);
	}

}
