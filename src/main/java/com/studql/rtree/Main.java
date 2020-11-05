package com.studql.rtree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.studql.rtree.callables.TreeDeleteCallable;
import com.studql.rtree.callables.TreeInsertCallable;
import com.studql.rtree.callables.TreeKNNSearchCallable;
import com.studql.rtree.callables.TreeRangeSearchCallable;
import com.studql.rtree.callables.TreeSearchCallable;
import com.studql.rtree.node.QuadraticSplitter;
import com.studql.shape.Point;
import com.studql.shape.Rectangle;
import com.studql.utils.Benchmark;
import com.studql.utils.Pair;
import com.studql.utils.Record;
import com.studql.utils.Visualizer;

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
			v.createVisualization(tree, new File("C:\\Users\\alzajac\\Downloads\\test.png"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		// create data points
		Point[] points = Benchmark.generateRandomPoints(10000000, new float[] { 0, 1000 }, new float[] { 100, 1000 });
//		try {
//			FileWriter f = new FileWriter("C:\\Users\\alzajac\\Downloads\\100_000_000_random.txt");
//			for (Point p : points) {
//				f.write(p.getX() + " " + p.getY() + "\n");
//			}
//			f.close();
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		List<Record<Point>> records = Benchmark.generateRecordsPoints(points);
		for (Record<Point> r : records) {
			tree.insert(r);
		}
		System.out.println(tree.toString());

		Visualizer<Point> v = new Visualizer<Point>();
		try {
			v.createVisualization(tree, new File("C:\\Users\\alzajac\\Downloads\\test.png"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void test_multithread_knnSearch(int num_datapoints, int num_search_points, int divideFactor,
			float[] xRange, float[] yRange, int k) throws InterruptedException {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// create search data points
		Rectangle[] searchRectangles = Benchmark.generateRandomRectangles(num_search_points, xRange, yRange);
		List<Record<Rectangle>> searchRecords = Benchmark.generateRecordsRectangle(searchRectangles);

		// init tree and Executor
		RtreeMulti<Rectangle> tree = new RtreeMulti<Rectangle>();
		tree.insert(records);
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<List<List<Pair<Record<Rectangle>, Float>>>>> results = null;
		List<TreeKNNSearchCallable<Rectangle>> tasks = new ArrayList<TreeKNNSearchCallable<Rectangle>>();

		// submit tasks
		for (int i = 0; i < num_search_points / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Record<Rectangle>> slicedRecords = new ArrayList<Record<Rectangle>>(
					searchRecords.subList(startIndex, endIndex));
			tasks.add(new TreeKNNSearchCallable<Rectangle>(tree, i, slicedRecords, k));
		}
		long start = System.currentTimeMillis();
		long end = 0;
		try {
			results = pool.invokeAll(tasks);
			end = System.currentTimeMillis();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();

		// get results
		for (Future<List<List<Pair<Record<Rectangle>, Float>>>> result : results) {
			List<List<Pair<Record<Rectangle>, Float>>> v = null;
			try {
				v = result.get();
//				System.out.println("Search operations: " + v);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		// print results
//		System.out.println(tree.toString());
		System.out.printf("time multi-thread: %d ms\n\n", end - start);
	}

	public static void test_multithread_rangeSearch(int num_datapoints, int num_search_points, int divideFactor,
			float[] xRange, float[] yRange) throws InterruptedException {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// create search data points
		List<Rectangle> searchRectangles = Arrays
				.asList(Benchmark.generateRandomRectangles(num_search_points, xRange, yRange));
		;

		// init tree and Executor
		RtreeMulti<Rectangle> tree = new RtreeMulti<Rectangle>();
		tree.insert(records);
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<List<List<Record<Rectangle>>>>> results = null;
		List<TreeRangeSearchCallable<Rectangle>> tasks = new ArrayList<TreeRangeSearchCallable<Rectangle>>();

		// submit tasks
		for (int i = 0; i < num_search_points / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Rectangle> slicedRecords = new ArrayList<Rectangle>(searchRectangles.subList(startIndex, endIndex));
			tasks.add(new TreeRangeSearchCallable<Rectangle>(tree, i, slicedRecords));
		}
		long start = System.currentTimeMillis();
		long end = 0;
		try {
			results = pool.invokeAll(tasks);
			end = System.currentTimeMillis();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();

		// get results
		for (Future<List<List<Record<Rectangle>>>> result : results) {
			List<List<Record<Rectangle>>> v = null;
			try {
				v = result.get();
//				System.out.println("Search operations: " + v);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		// print results
//		System.out.println(tree.toString());
		System.out.printf("time multi-thread: %d ms\n\n", end - start);
	}

	public static void test_multithread_search(int num_datapoints, int num_search_points, int divideFactor,
			float[] xRange, float[] yRange) throws InterruptedException {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// create search data points
		Rectangle[] searchRectangles = Benchmark.generateRandomRectangles(num_search_points, xRange, yRange);
		List<Record<Rectangle>> searchRecords = Benchmark.generateRecordsRectangle(searchRectangles);

		// init tree and Executor
		RtreeMulti<Rectangle> tree = new RtreeMulti<Rectangle>();
		tree.insert(records);
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<List<Record<Rectangle>>>> results = null;
		List<TreeSearchCallable<Rectangle>> tasks = new ArrayList<TreeSearchCallable<Rectangle>>();

		// submit tasks
		for (int i = 0; i < num_search_points / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Record<Rectangle>> slicedRecords = new ArrayList<Record<Rectangle>>(
					searchRecords.subList(startIndex, endIndex));
			tasks.add(new TreeSearchCallable<Rectangle>(tree, i, slicedRecords));
		}
		long start = System.currentTimeMillis();
		long end = 0;
		try {
			results = pool.invokeAll(tasks);
			end = System.currentTimeMillis();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();

		// get results
		for (Future<List<Record<Rectangle>>> result : results) {
			List<Record<Rectangle>> v = null;
			try {
				v = result.get();
//				System.out.println("Search operations: " + v);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		// print results
//		System.out.println(tree.toString());
		System.out.printf("time multi-thread: %d ms\n\n", end - start);
	}

	public static void test_multithread_delete(int num_datapoints, int divideFactor, float[] xRange, float[] yRange)
			throws InterruptedException {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// init tree and Executor
		RtreeMulti<Rectangle> tree = new RtreeMulti<Rectangle>();
		tree.insert(records);
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<List<Boolean>>> results = null;
		List<TreeDeleteCallable<Rectangle>> tasks = new ArrayList<TreeDeleteCallable<Rectangle>>();

		// submit tasks
		for (int i = 0; i < num_datapoints / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Record<Rectangle>> slicedRecords = new ArrayList<Record<Rectangle>>(
					records.subList(startIndex, endIndex));
			tasks.add(new TreeDeleteCallable<Rectangle>(tree, i, slicedRecords));
		}
		long start = System.currentTimeMillis();
		long end = 0;
		try {
			results = pool.invokeAll(tasks);
			end = System.currentTimeMillis();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();

		// get results
		for (Future<List<Boolean>> result : results) {
			List<Boolean> v = null;
			try {
				v = result.get();
//				System.out.println("Delete operations: " + v);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		// print results
//		System.out.println(tree.toString());
		System.out.printf("time multi-thread: %d ms\n\n", end - start);
	}

	public static void test_multithread_insert(int num_datapoints, int divideFactor, float[] xRange, float[] yRange)
			throws InterruptedException {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// init tree and Executor
		RtreeMulti<Rectangle> tree = new RtreeMulti<Rectangle>();
		ExecutorService pool = Executors.newCachedThreadPool();
		List<TreeInsertCallable<Rectangle>> tasks = new ArrayList<TreeInsertCallable<Rectangle>>();

		// submit tasks
		for (int i = 0; i < num_datapoints / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Record<Rectangle>> slicedRecords = new ArrayList<Record<Rectangle>>(
					records.subList(startIndex, endIndex));
			tasks.add(new TreeInsertCallable<Rectangle>(tree, i, slicedRecords));
		}
		long start = System.currentTimeMillis();
		long end = 0;
		try {
			pool.invokeAll(tasks);
			end = System.currentTimeMillis();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pool.shutdown();

		// print results and cleanup
//		System.out.println(tree.toString());
		System.out.printf("time multi-thread: %d ms\n\n", end - start);
	}

	public static void test_single_insert(int num_datapoints, float[] xRange, float[] yRange) {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// init tree and Executor
		Rtree<Rectangle> tree = new Rtree<Rectangle>();

		// test single thread
		long start = System.currentTimeMillis();
		tree.insert(records);
		long end = System.currentTimeMillis();
//		System.out.println(tree.toString());
		System.out.printf("time single-thread: %d ms\n\n", end - start);
	}

	public static void test_single_delete(int num_datapoints, float[] xRange, float[] yRange) {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// init tree and Executor
		Rtree<Rectangle> tree = new Rtree<Rectangle>();

		// test single thread
		tree.insert(records);
		long start = System.currentTimeMillis();
		tree.delete(records);
		long end = System.currentTimeMillis();
		System.out.println(tree.toString());
		System.out.printf("time single-thread: %d ms\n\n", end - start);
	}

	public static void test_single_search(int num_datapoints, int num_search_points, int divideFactor, float[] xRange,
			float[] yRange) {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// create search data points
		Rectangle[] searchRectangles = Benchmark.generateRandomRectangles(num_search_points, xRange, yRange);
		List<Record<Rectangle>> searchRecords = Benchmark.generateRecordsRectangle(searchRectangles);

		// init tree and Executor
		Rtree<Rectangle> tree = new Rtree<Rectangle>();

		// test single thread
		tree.insert(records);
		List<List<Record<Rectangle>>> searchResults = new ArrayList<List<Record<Rectangle>>>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < num_search_points / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Record<Rectangle>> slicedRecords = new ArrayList<Record<Rectangle>>(
					searchRecords.subList(startIndex, endIndex));
			searchResults.add(tree.search(slicedRecords));
		}
		long end = System.currentTimeMillis();
//		System.out.println(tree.toString());
//		System.out.println(searchResults);
		System.out.printf("time single-thread: %d ms\n\n", end - start);
	}

	public static void test_single_rangeSearch(int num_datapoints, int num_search_points, int divideFactor,
			float[] xRange, float[] yRange) {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// create search data points
		List<Rectangle> searchRectangles = Arrays
				.asList(Benchmark.generateRandomRectangles(num_search_points, xRange, yRange));

		// init tree and Executor
		Rtree<Rectangle> tree = new Rtree<Rectangle>();

		// test single thread
		tree.insert(records);
		List<List<List<Record<Rectangle>>>> searchResults = new ArrayList<List<List<Record<Rectangle>>>>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < num_search_points / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Rectangle> slicedRecords = new ArrayList<Rectangle>(searchRectangles.subList(startIndex, endIndex));
			searchResults.add(tree.rangeSearch(slicedRecords));
		}
		long end = System.currentTimeMillis();
//		System.out.println(tree.toString());
//		System.out.println(searchResults);
		System.out.printf("time single-thread: %d ms\n\n", end - start);
	}

	public static void test_single_knnSearch(int num_datapoints, int num_search_points, int divideFactor,
			float[] xRange, float[] yRange, int k) {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");

		// create data points
		Rectangle[] rectangles = Benchmark.generateRandomRectangles(num_datapoints, xRange, yRange);
		List<Record<Rectangle>> records = Benchmark.generateRecordsRectangle(rectangles);

		// create search data points
		Rectangle[] searchRectangles = Benchmark.generateRandomRectangles(num_search_points, xRange, yRange);
		List<Record<Rectangle>> searchRecords = Benchmark.generateRecordsRectangle(searchRectangles);

		// init tree and Executor
		Rtree<Rectangle> tree = new Rtree<Rectangle>();

		// test single thread
		tree.insert(records);
		List<List<List<Pair<Record<Rectangle>, Float>>>> searchResults = new ArrayList<List<List<Pair<Record<Rectangle>, Float>>>>();
		long start = System.currentTimeMillis();
		for (int i = 0; i < num_search_points / divideFactor; i++) {
			int startIndex = i * divideFactor, endIndex = (i + 1) * divideFactor;
			List<Record<Rectangle>> slicedRecords = new ArrayList<Record<Rectangle>>(
					searchRecords.subList(startIndex, endIndex));
			searchResults.add(tree.nearestNeighborsSearch(slicedRecords, k));
		}
		long end = System.currentTimeMillis();
//		System.out.println(tree.toString());
//		System.out.println(searchResults);
		System.out.printf("time single-thread: %d ms\n\n", end - start);
	}

	public static void completeSingleBenchmark() {
		Benchmark b = new Benchmark("D:\\Users\\utilisateur\\Downloads\\test.png");
		int n = 10000;
		float[] xRange = new float[] { 0, 400 };
		float[] yRange = new float[] { 0, 600 };
		int[] page_sizes = new int[] { 6, 12, 25, 50, 102 };
		List<Function<Integer, Integer>> min_page_operators = Arrays.asList(num -> Math.round(num) / 3,
				num -> Math.round(num) / 2, num -> 2);
		boolean shouldVisualize = true;
		String fileLocation = "C:\\Users\\alzajac\\Downloads\\1000.txt";
		b.benchmarkInsertWithRandomRectangles(n, xRange, yRange, page_sizes, min_page_operators, shouldVisualize);
		b.benchmarkInsertWithDatasetPoints(fileLocation, page_sizes, min_page_operators, shouldVisualize);
	}

	public static void main(String[] args) {
		test_points();
		int n = 100;
		int n_search = 100000;
		int divideFactor = 10000;
		int k = 3;
		float[] xRange = new float[] { 0, 500 };
		float[] yRange = new float[] { -100, 600 };
		test_single_insert(n, xRange, yRange);
		try {
			test_multithread_insert(n, divideFactor, xRange, yRange);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}