package src.main.java.com.studql.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import src.main.java.com.studql.rtree.NodeSplitter;
import src.main.java.com.studql.rtree.QuadraticSplitter;
import src.main.java.com.studql.rtree.LinearSplitter;
import src.main.java.com.studql.rtree.Record;
import src.main.java.com.studql.rtree.Rtree;
import src.main.java.com.studql.shape.Point;
import src.main.java.com.studql.shape.Rectangle;

public class Benchmark{
	private String fileLocation;
	public Benchmark(String fileLocation) {
		this.fileLocation = fileLocation;
	}

	private int getRandomInRange(int min, int max) {
		return (int) ((Math.random() * (max - min)) + min);
	}

	private Point[] generateRandomPoints(int n, int[] xRange, int[] yRange) {
		Point[] dataPoints = new Point[n];
		for (int i = 0; i < n; ++i) {
			int x = this.getRandomInRange(xRange[0], xRange[1]);
			int y = this.getRandomInRange(yRange[0], yRange[1]);
			dataPoints[i] = new Point(x, y);
		}
		return dataPoints;
	}

	private Rectangle[] generateRandomRectangles(int n, int[] xRange, int[] yRange) {
		Rectangle[] dataPoints = new Rectangle[n];
		for (int i = 0; i < n; ++i) {
			int xBottomRight = this.getRandomInRange(xRange[0], xRange[1]);
			int yBottomRight = this.getRandomInRange(yRange[0], yRange[1]);
			int xTopLeft = this.getRandomInRange(xRange[0], xBottomRight);
			int yTopLeft = this.getRandomInRange(yBottomRight, yRange[1]);
			dataPoints[i] = new Rectangle(new Point(xTopLeft, yTopLeft), new Point(xBottomRight, yBottomRight));
		}
		return dataPoints;
	}

	public void benchmarkInsertWithRandomPoints(int num_datapoints, int[] xRange, int[] yRange, int[] page_sizes,
			List<Function<Integer, Integer>> min_size_operations, boolean shouldVisualize) {
		// generate random records
		Point[] dataPoints = this.generateRandomPoints(num_datapoints, xRange, yRange);
		ArrayList<Record<Point>> records = new ArrayList<Record<Point>>();
		for (int i = 0; i < dataPoints.length; i++)
			records.add(new Record<Point>(dataPoints[i], Integer.toString(i)));
		launchPointBenchmark(page_sizes, min_size_operations, shouldVisualize, records);
	}

	public void benchmarkInsertWithDatasetPoints(String fileLocation, int[] page_sizes,
			List<Function<Integer, Integer>> min_size_operations, boolean shouldVisualize) {
		// generate random records
		ArrayList<Record<Point>> records = this.generatePointRecordsFromDataset(fileLocation);
		launchPointBenchmark(page_sizes, min_size_operations, shouldVisualize, records);
	}

	public void benchmarkInsertWithRandomRectangles(int num_datapoints, int[] xRange, int[] yRange, int[] page_sizes,
			List<Function<Integer, Integer>> min_size_operations, boolean shouldVisualize) {
		// generate random records
		Rectangle[] dataPoints = this.generateRandomRectangles(num_datapoints, xRange, yRange);
		ArrayList<Record<Rectangle>> records = new ArrayList<Record<Rectangle>>();
		for (int i = 0; i < dataPoints.length; i++)
			records.add(new Record<Rectangle>(dataPoints[i], Integer.toString(i)));
		launchRectangleBenchmark(page_sizes, min_size_operations, shouldVisualize, records);
	}

	private void launchRectangleBenchmark(int[] page_sizes, List<Function<Integer, Integer>> min_size_operations,
			boolean shouldVisualize, ArrayList<Record<Rectangle>> records) {
		// generate combinations of Rtrees
		for (int max_page_size : page_sizes) {
			for (var operation : min_size_operations) {
				int min_page_size = operation.apply(max_page_size);
				NodeSplitter<Rectangle> splitter = new LinearSplitter<Rectangle>(min_page_size);
				Rtree<Rectangle> tree = new Rtree<Rectangle>(min_page_size, max_page_size, splitter);
				for (Record<Rectangle> record : records) {
					tree.insert(record);
				}
				if (shouldVisualize) {
					Visualizer<Rectangle> v = new Visualizer<Rectangle>();
					try {
						String splitter_type = splitter instanceof LinearSplitter<?> ? "Quadratic" : "Linear";
						v.createVisualization(tree, new File(this.fileLocation + "m=" + min_page_size
								+ "M=" + max_page_size + "splitter=" + splitter_type + ".png"));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}
		}
	}

	private void launchPointBenchmark(int[] page_sizes, List<Function<Integer, Integer>> min_size_operations,
			boolean shouldVisualize, ArrayList<Record<Point>> records) {
		// generate combinations of Rtrees
		for (int max_page_size : page_sizes) {
			for (var operation : min_size_operations) {
				int min_page_size = operation.apply(max_page_size);
				NodeSplitter<Point> splitter = new LinearSplitter<Point>(min_page_size);
				Rtree<Point> tree = new Rtree<Point>(min_page_size, max_page_size, splitter);
				for (Record<Point> record : records) {
					tree.insert(record);
				}
				if (shouldVisualize) {
					Visualizer<Point> v = new Visualizer<Point>();
					try {
						String splitter_type = splitter instanceof LinearSplitter<?> ? "Quadratic" : "Linear";
						v.createVisualization(tree, new File(this.fileLocation + "m=" + min_page_size
								+ "M=" + max_page_size + "splitter=" + splitter_type + ".png"));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			}
		}
	}

	public ArrayList<Record<Point>> generatePointRecordsFromDataset(String fileLocation) {
		BufferedReader reader;
		ArrayList<Record<Point>> result = new ArrayList<Record<Point>>();
		try {
			reader = new BufferedReader(new FileReader(fileLocation));
			String line = reader.readLine();
			int i = 0;
			while (line != null) {
				String[] splittedLine = line.split(" ");
				if (splittedLine.length >= 2) {
					float x = Float.parseFloat(splittedLine[0]), y = Float.parseFloat(splittedLine[1]);
					result.add(new Record<Point>(new Point(x, y), Integer.toString(i)));
				}
				// read next line
				line = reader.readLine();
				++i;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}
