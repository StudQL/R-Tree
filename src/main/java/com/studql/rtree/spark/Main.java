package com.studql.rtree.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.studql.rtree.node.NodeSplitter;
import com.studql.rtree.node.QuadraticSplitter;
import com.studql.rtree.spark.datamapper.PointDataMapper;
import com.studql.rtree.spark.queries.QueryExecutor;
import com.studql.rtree.spark.rdd.PointRDD;
import com.studql.shape.Point;
import com.studql.shape.Rectangle;
import com.studql.utils.Benchmark;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class Main {

	public static void testSearchQuery(PointRDD dataset, int num_queries, float[] xRange, float[] yRange) {
		// generate datapoints for knn search
		Point[] points = Benchmark.generateRandomPoints(num_queries, xRange, yRange);
		List<Record<Point>> searchRecords = Benchmark.generateRecordsPoints(points);

		boolean withIndex = false;
		boolean withInitialData = true;
		while (true) {
			for (Record<Point> searchRecord : searchRecords) {
				JavaRDD<Record<Point>> matchingRecords = QueryExecutor.exactQuery(dataset, searchRecord, withIndex,
						withInitialData);
				for (Record<Point> matchingRecord : matchingRecords.collect()) {
					System.out.println(matchingRecord);
				}
			}
		}
	}

	public static void testRangeQuery(PointRDD dataset, int num_queries, float[] xRange, float[] yRange) {
		// generate datapoints for knn search
		Rectangle[] ranges = Benchmark.generateRandomRectangles(num_queries, xRange, yRange);
		boolean withIndex = true;
		boolean withInitialData = true;
		while (true) {
			for (Rectangle rangeRectangle : ranges) {
				JavaRDD<Record<Point>> matchingRecords = QueryExecutor.rangeQuery(dataset, rangeRectangle, withIndex,
						withInitialData);
				for (Record<Point> matchingRecord : matchingRecords.collect()) {
					System.out.println(matchingRecord);
				}
			}
		}
	}

	public static void testKnnQuery(PointRDD dataset, int num_queries, float[] xRange, float[] yRange) {
		// generate datapoints for knn search
		Point[] centerPoints = Benchmark.generateRandomPoints(num_queries, xRange, yRange);
		List<Record<Point>> knnCenterRecords = Benchmark.generateRecordsPoints(centerPoints);

		boolean withIndex = false;
		boolean withInitialData = true;
		int k = 8;
		for (Record<Point> centerRecord : knnCenterRecords) {
			List<Pair<Record<Point>, Float>> result = QueryExecutor.knnQuery(dataset, centerRecord, k, withIndex,
					withInitialData);
			for (Pair<Record<Point>, Float> r : result) {
				System.out.println(r.toString());
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
		int max_num_records = 512;
		int min_num_records = 512 / 2;
		NodeSplitter<Point> splitter = new QuadraticSplitter<Point>(min_num_records);
		// instantiating the RDDpoint
		PointRDD dataset = new PointRDD(sc, fileLocation, mapper, min_num_records, max_num_records, splitter);
		// test query type
		int num_queries = 10;
//		testSearchQuery(dataset, num_queries, xRange, yRange);
		testRangeQuery(dataset, num_queries, xRange, yRange);
//		testKnnQuery(dataset, num_queries, xRange, yRange);
	}

}
