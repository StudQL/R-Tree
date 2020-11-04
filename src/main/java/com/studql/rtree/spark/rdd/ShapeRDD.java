package com.studql.rtree.spark.rdd;

import org.apache.spark.api.java.JavaRDD;

import com.studql.rtree.Rtree;
import com.studql.shape.Boundable;

public abstract class ShapeRDD<T extends Boundable> {

	protected JavaRDD<T> initialRDD;

	protected JavaRDD<Rtree<T>> indexedInitialRDD;

	protected JavaRDD<T> spatialRDD;

	protected JavaRDD<Rtree<T>> indexedSpatialRDD;

	public JavaRDD<Rtree<T>> getindexedInitielRDD() {
		return indexedInitialRDD;
	}

	public JavaRDD<Rtree<T>> getindexedSpatialRDD() {
		return indexedSpatialRDD;
	}
}
