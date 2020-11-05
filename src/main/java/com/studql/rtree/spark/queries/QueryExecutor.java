package com.studql.rtree.spark.queries;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.studql.rtree.spark.rdd.ShapeRDD;
import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class QueryExecutor<T extends Boundable> {
	public static <T extends Boundable> JavaRDD<Record<T>> exactQuery(ShapeRDD<T> shapeRDD, Record<T> searchRecord,
			boolean withIndex, boolean withInitialData) {
		if (withIndex) {
			if (withInitialData)
				return shapeRDD.getIndexedInitialRDD().mapPartitions(new ExactQueryFilterWithIndex<T>(searchRecord));
			return shapeRDD.getIndexedSpatialRDD().mapPartitions(new ExactQueryFilterWithIndex<T>(searchRecord));
		} else if (withInitialData)
			return shapeRDD.getInitialRDD().filter(new ExactQueryFilter<T>(searchRecord));
		return shapeRDD.getSpatialRDD().filter(new ExactQueryFilter<T>(searchRecord));
	}

	public static <T extends Boundable> JavaRDD<Record<T>> rangeQuery(ShapeRDD<T> shapeRDD, Rectangle searchRange,
			boolean withIndex, boolean withInitialData) {
		if (withIndex) {
			if (withInitialData)
				return shapeRDD.getIndexedInitialRDD().mapPartitions(new RangeQueryFilterWithIndex<T>(searchRange));
			return shapeRDD.getIndexedSpatialRDD().mapPartitions(new RangeQueryFilterWithIndex<T>(searchRange));
		} else if (withInitialData)
			return shapeRDD.getInitialRDD().filter(new RangeQueryFilter<T>(searchRange));
		return shapeRDD.getSpatialRDD().filter(new RangeQueryFilter<T>(searchRange));
	}

	public static <T extends Boundable> List<Pair<Record<T>, Float>> knnQuery(ShapeRDD<T> shapeRDD,
			Record<T> centerRecord, int k, boolean withIndex, boolean withInitialData) {
		JavaRDD<Pair<Record<T>, Float>> partitionResult;
		if (withIndex) {
			if (withInitialData)
				partitionResult = shapeRDD.getIndexedInitialRDD()
						.mapPartitions(new KnnQueryFilterWithIndex<T>(k, centerRecord));
			else
				partitionResult = shapeRDD.getIndexedSpatialRDD()
						.mapPartitions(new KnnQueryFilterWithIndex<T>(k, centerRecord));
		} else if (withInitialData)
			partitionResult = shapeRDD.getInitialRDD().mapPartitions(new KnnQueryFilter<T>(k, centerRecord));
		else
			partitionResult = shapeRDD.getSpatialRDD().mapPartitions(new KnnQueryFilter<T>(k, centerRecord));
		// we need to take the top k from each collected partition to be correct
		return partitionResult.takeOrdered(k, new KnnDistanceComparator<T>());
	}
}
