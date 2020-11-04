package com.studql.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.studql.rtree.Rtree;
import com.studql.rtree.node.LinearSplitter;
import com.studql.rtree.node.NodeSplitter;
import com.studql.shape.Point;
import com.studql.shape.Rectangle;

public class Benchmark {
	private String fileLocation;

	public Benchmark(String fileLocation) {
		this.fileLocation = fileLocation;
	}

	public static int getRandomInRange(int min, int max) {
		return (int) ((Math.random() * (max - min)) + min);
	}

	public static float interpolatePoint(float value, float[] valueRange, float[] referenceRange) {
		return referenceRange[0]
				+ (value - valueRange[0]) * ((referenceRange[1] - referenceRange[0]) / (valueRange[1] - valueRange[0]));
	}

	public static float interpolateLine(float value, float[] valueRange, float[] referenceRange) {
		return (value / (valueRange[1] - valueRange[0])) * (referenceRange[1] - referenceRange[0]);
	}

	public static Point[] generateRandomPoints(int n, int[] xRange, int[] yRange) {
		Point[] dataPoints = new Point[n];
		for (int i = 0; i < n; ++i) {
			int x = getRandomInRange(xRange[0], xRange[1]);
			int y = getRandomInRange(yRange[0], yRange[1]);
			dataPoints[i] = new Point(x, y);
		}
		return dataPoints;
	}

	public static Rectangle[] generateRandomRectangles(int n, int[] xRange, int[] yRange) {
		Rectangle[] dataPoints = new Rectangle[n];
		for (int i = 0; i < n; ++i) {
			int xBottomRight = getRandomInRange(xRange[0], xRange[1]);
			int yBottomRight = getRandomInRange(yRange[0], yRange[1]);
			int xTopLeft = getRandomInRange(xRange[0], xBottomRight);
			int yTopLeft = getRandomInRange(yBottomRight, yRange[1]);
			dataPoints[i] = new Rectangle(new Point(xTopLeft, yTopLeft), new Point(xBottomRight, yBottomRight));
		}
		return dataPoints;
	}

	public List<Record<Rectangle>> generateRecordsRectangle(Rectangle[] rectangles) {
		List<Record<Rectangle>> records = new ArrayList<Record<Rectangle>>();
		int id = 0;
		for (Rectangle r : rectangles) {
			records.add(new Record<Rectangle>(r, Integer.toString(id++)));
		}
		return records;
	}

	public List<Record<Point>> generateRecordsPoints(Point[] points) {
		List<Record<Point>> records = new ArrayList<Record<Point>>();
		int id = 0;
		for (Point p : points) {
			records.add(new Record<Point>(p, Integer.toString(id++)));
		}
		return records;
	}

	public void benchmarkInsertWithRandomPoints(int num_datapoints, int[] xRange, int[] yRange, int[] page_sizes,
			List<Function<Integer, Integer>> min_size_operations, boolean shouldVisualize) {
		// generate random records
		Point[] dataPoints = generateRandomPoints(num_datapoints, xRange, yRange);
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
		Rectangle[] dataPoints = generateRandomRectangles(num_datapoints, xRange, yRange);
		ArrayList<Record<Rectangle>> records = new ArrayList<Record<Rectangle>>();
		for (int i = 0; i < dataPoints.length; i++)
			records.add(new Record<Rectangle>(dataPoints[i], Integer.toString(i)));
		launchRectangleBenchmark(page_sizes, min_size_operations, shouldVisualize, records);
	}

	private void launchRectangleBenchmark(int[] page_sizes, List<Function<Integer, Integer>> min_size_operations,
			boolean shouldVisualize, ArrayList<Record<Rectangle>> records) {
		// generate combinations of Rtrees
		for (int max_page_size : page_sizes) {
			for (Function<Integer, Integer> operation : min_size_operations) {
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
						v.createVisualization(tree, new File(this.fileLocation + "m=" + min_page_size + "M="
								+ max_page_size + "splitter=" + splitter_type + ".png"));
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
			for (Function<Integer, Integer> operation : min_size_operations) {
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
						v.createVisualization(tree, new File(this.fileLocation + "m=" + min_page_size + "M="
								+ max_page_size + "splitter=" + splitter_type + ".png"));
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
