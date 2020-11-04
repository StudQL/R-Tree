package com.studql.rtree.spark.rdd;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.studql.rtree.node.NodeSplitter;
import com.studql.rtree.spark.RtreeIndexBuilder;
import com.studql.rtree.spark.datamapper.RectangleDataMapper;
import com.studql.shape.Rectangle;

public class RectangleRDD extends ShapeRDD<Rectangle> implements Serializable {

	private static final long serialVersionUID = 7695506295701313960L;

	public RectangleRDD(JavaSparkContext sc, String inputLocation, RectangleDataMapper dataMapper, int min_num_records,
			int max_num_records, NodeSplitter<Rectangle> splitter) {
		JavaRDD<String> dataLines = sc.textFile(inputLocation);
		// loading RDD with respect to input data
		this.initialRDD = dataLines.mapPartitions(dataMapper);
		// building one rtree for each partition
		RtreeIndexBuilder<Rectangle> indexBuilder = new RtreeIndexBuilder<Rectangle>(min_num_records, max_num_records,
				splitter);
		this.indexedInitialRDD = this.initialRDD.mapPartitions(indexBuilder);
	}

}
