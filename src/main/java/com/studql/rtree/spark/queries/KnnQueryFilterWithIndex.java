package com.studql.rtree.spark.queries;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.rtree.Rtree;
import com.studql.shape.Boundable;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class KnnQueryFilterWithIndex<T extends Boundable>
		implements FlatMapFunction<Iterator<Rtree<T>>, Pair<Record<T>, Float>> {

	private static final long serialVersionUID = 778142137228618884L;
	private int k;
	private Record<T> queryCenter;

	public KnnQueryFilterWithIndex(int k, Record<T> queryCenter) {
		this.k = k;
		this.queryCenter = queryCenter;
	}

	// implement KNN for Iterator of records
	@Override
	public Iterator<Pair<Record<T>, Float>> call(Iterator<Rtree<T>> rTreesIndexes) {
		if (rTreesIndexes.hasNext()) {
			Rtree<T> treeIndex = rTreesIndexes.next();
			List<Pair<Record<T>, Float>> knnResults = treeIndex.nearestNeighborsSearch(queryCenter, k);
			return knnResults.iterator();
		}
		return null;
	}
}
