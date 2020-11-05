package com.studql.rtree.spark.rdd;

import org.apache.spark.api.java.JavaRDD;

import com.studql.rtree.Rtree;
import com.studql.shape.Boundable;
import com.studql.utils.Record;

public abstract class ShapeRDD<T extends Boundable> {

	protected JavaRDD<Record<T>> initialRDD;

	protected JavaRDD<Rtree<T>> indexedInitialRDD;

	protected JavaRDD<Record<T>> spatialRDD;

	protected JavaRDD<Rtree<T>> indexedSpatialRDD;

	public JavaRDD<Rtree<T>> getIndexedInitialRDD() {
		return indexedInitialRDD;
	}

	public JavaRDD<Rtree<T>> getIndexedSpatialRDD() {
		return indexedSpatialRDD;
	}

	public JavaRDD<Record<T>> getInitialRDD() {
		return initialRDD;
	}

	public JavaRDD<Record<T>> getSpatialRDD() {
		return spatialRDD;
	}
}
