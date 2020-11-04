package com.studql.rtree.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.studql.rtree.node.NodeSplitter;
import com.studql.rtree.node.QuadraticSplitter;
import com.studql.rtree.spark.datamapper.PointDataMapper;
import com.studql.rtree.spark.rdd.PointRDD;
import com.studql.shape.Point;
import com.studql.utils.Pair;

public class Main {

	public static void testSearchQuery(JavaSparkContext sc, String fileLocation, int num_queries) {
		// creating dataset properties
		String delimiter = " ";
		int[] coordinatesPositions = new int[] { 0, 1 };
		float[] xRange = null;
		float[] yRange = null;
		Pair<float[], float[]> rangeInterpolators = new Pair<float[], float[]>(xRange, yRange);
		boolean hasHeader = false;
		PointDataMapper mapper = new PointDataMapper(delimiter, coordinatesPositions, rangeInterpolators, hasHeader);
		int max_num_records = 512;
		int min_num_records = 512 / 2;
		NodeSplitter<Point> splitter = new QuadraticSplitter<Point>(min_num_records);
		// instantiating the RDDpoint
		PointRDD dataset = new PointRDD(sc, fileLocation, mapper, min_num_records, max_num_records, splitter);
//		Point[] points = Benchamrk.generateRandomPoints(num_queries, new int[] { 0, 1000 }, new int[] { 0, 1000 });
//		List<Record<Point>> records = b.generateRecordsPoints(points);
//		for (Record<Point> record : records) {
//
//		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("SparkRtreeProcessing").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String fileLocation = "hdfs://localhost:9000/test/8_test.txt";
		int num_queries = 10;
		testSearchQuery(sc, fileLocation, num_queries);
		while (true) {

		}
	}

}
