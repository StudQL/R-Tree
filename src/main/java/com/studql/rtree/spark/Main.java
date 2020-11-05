package com.studql.rtree.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.studql.rtree.node.NodeSplitter;
import com.studql.rtree.node.QuadraticSplitter;
import com.studql.rtree.spark.datamapper.PointDataMapper;
import com.studql.shape.Point;
import com.studql.shape.Rectangle;
import com.studql.utils.Benchmark;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class Main {

	public static void testSearchQuery(ShapeRDD<Point> dataset, int num_queries, float[] xRange, float[] yRange,
			int msDelay) {
		// generate datapoints for knn search
		Point[] points = Benchmark.generateRandomPoints(num_queries, xRange, yRange);
		List<Record<Point>> searchRecords = Benchmark.generateRecordsPoints(points);

		boolean withIndex = false;
		boolean withInitialData = true;
		while (true) {
			Record<Point> searchRecord = new Record<Point>(Benchmark.generateRandomPoint(xRange, yRange), "1");
			JavaRDD<Record<Point>> matchingRecords = dataset.exactQuery(searchRecord, withIndex, withInitialData);
			for (Record<Point> matchingRecord : matchingRecords.collect()) {
//				System.out.println(matchingRecord);
			}

			try {
				Thread.sleep(msDelay);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void testRangeQuery(ShapeRDD<Point> dataset, int num_queries, double selectivityFactor,
			float[] xRange, float[] yRange, int msDelay) {
		// generate datapoints for knn search
		boolean withIndex = false;
		boolean withInitialData = false;
		float xMin = xRange[0], xMax = xRange[1], yMin = yRange[0], yMax = yRange[1];
		while (true) {
			double selectivity = 1;
			for (int i = 0; i < 4; i++) {
				float[] selectiveXRange = new float[] { (float) (xMin * selectivity), (float) (xMax * selectivity) };
				float[] selectiveYRange = new float[] { (float) (yMin * selectivity), (float) (yMax * selectivity) };
				Rectangle rangeRectangle = Benchmark.generateRandomRectangle(selectiveXRange, selectiveYRange);
				JavaRDD<Record<Point>> matchingRecords = dataset.rangeQuery(rangeRectangle, withIndex, withInitialData);
				for (Record<Point> matchingRecord : matchingRecords.collect()) {
//					System.out.println(matchingRecord);
				}
				try {
					Thread.sleep(msDelay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// reduce the query MBR
				selectivity *= selectivityFactor;
			}
		}
	}

	public static void testKnnQuery(ShapeRDD<Point> dataset, int[] knnValues, float[] xRange, float[] yRange,
			int msDelay) {
		// generate datapoints for knn search
		boolean withIndex = false;
		boolean withInitialData = true;
		while (true) {
			Record<Point> centerRecord = new Record<Point>(Benchmark.generateRandomPoint(xRange, yRange), "1");
			for (int k : knnValues) {
				List<Pair<Record<Point>, Float>> result = dataset.knnQuery(centerRecord, k, withIndex, withInitialData);
				for (Pair<Record<Point>, Float> r : result) {
//					System.out.println(r.toString());
				}
				try {
					Thread.sleep(msDelay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("SparkRtreeProcessing").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String fileLocation = "hdfs://localhost:9000/test/10_000_random.txt";
		// creating dataset properties
		String delimiter = " ";
		int[] coordinatesPositions = new int[] { 0, 1 };
		float[] xRange = new float[] { 0, 1000 };
		float[] yRange = new float[] { 0, 1000 };
		float[] xRangeInterpolation = null;
		float[] yRangeInterpolation = null;
		Pair<float[], float[]> rangeInterpolators = new Pair<float[], float[]>(xRangeInterpolation,
				yRangeInterpolation);
		boolean hasHeader = false;
		PointDataMapper mapper = new PointDataMapper(delimiter, coordinatesPositions, rangeInterpolators, hasHeader);
		int max_num_records = 1000;
		int min_num_records = 500;
		int num_partitions = Runtime.getRuntime().availableProcessors() * 3;
		NodeSplitter<Point> splitter = new QuadraticSplitter<Point>(min_num_records);
		// instantiating the RDDpoint
		ShapeRDD<Point> dataset = new ShapeRDD<Point>(sc, fileLocation, mapper, min_num_records, max_num_records,
				splitter, num_partitions);
		// test query type
		int num_queries = 10;
		int msDelay = 10000;
		double selectivityFactor = 0.25;
		int[] knnValues = new int[] { 1, 10, 100, 1000 };
//		testSearchQuery(dataset, num_queries, xRange, yRange, msDelay);
		testRangeQuery(dataset, num_queries, selectivityFactor, xRange, yRange, msDelay);
//		testKnnQuery(dataset, knnValues, xRange, yRange, msDelay);
	}

}
