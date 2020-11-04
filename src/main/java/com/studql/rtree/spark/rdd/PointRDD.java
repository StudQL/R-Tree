package com.studql.rtree.spark.rdd;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.studql.rtree.node.NodeSplitter;
import com.studql.rtree.spark.RtreeIndexBuilder;
import com.studql.rtree.spark.datamapper.PointDataMapper;
import com.studql.shape.Point;

public class PointRDD extends ShapeRDD<Point> implements Serializable {

	private static final long serialVersionUID = 5076258178899251324L;

	public PointRDD(JavaSparkContext sc, String inputLocation, PointDataMapper pointDataMapper, int min_num_records,
			int max_num_records, NodeSplitter<Point> splitter) {
		JavaRDD<String> dataLines = sc.textFile(inputLocation);
		// loading RDD with respect to input data
		this.initialRDD = dataLines.mapPartitions(pointDataMapper);
		// building one rtree for each partition
		RtreeIndexBuilder<Point> indexBuilder = new RtreeIndexBuilder<Point>(min_num_records, max_num_records,
				splitter);
		this.indexedInitialRDD = this.initialRDD.mapPartitions(indexBuilder);
	}

}
