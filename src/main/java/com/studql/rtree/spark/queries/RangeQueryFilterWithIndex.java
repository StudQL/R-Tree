package com.studql.rtree.spark.queries;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.rtree.Rtree;
import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Record;

public class RangeQueryFilterWithIndex<T extends Boundable> implements FlatMapFunction<Iterator<Rtree<T>>, Record<T>> {

	private static final long serialVersionUID = -2830205276434562634L;
	private Rectangle queryRange;

	public RangeQueryFilterWithIndex(Rectangle queryRange) {
		this.queryRange = queryRange;
	}

	@Override
	public Iterator<Record<T>> call(Iterator<Rtree<T>> rTreesIndexes) throws Exception {
		if (rTreesIndexes.hasNext()) {
			Rtree<T> treeIndex = rTreesIndexes.next();
			List<Record<T>> searchResult = treeIndex.rangeSearch(queryRange);
			return searchResult.iterator();
		}
		return null;
	}
}
