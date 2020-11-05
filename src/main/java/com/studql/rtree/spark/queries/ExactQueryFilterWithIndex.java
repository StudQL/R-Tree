package com.studql.rtree.spark.queries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.rtree.Rtree;
import com.studql.shape.Boundable;
import com.studql.utils.Record;

public class ExactQueryFilterWithIndex<T extends Boundable> implements FlatMapFunction<Iterator<Rtree<T>>, Record<T>> {

	private static final long serialVersionUID = 3066998330032732667L;
	private Record<T> queryRecord;

	public ExactQueryFilterWithIndex(Record<T> queryRecord) {
		this.queryRecord = queryRecord;
	}

	@Override
	public Iterator<Record<T>> call(Iterator<Rtree<T>> rTreesIndexes) throws Exception {
		if (rTreesIndexes.hasNext()) {
			Rtree<T> treeIndex = rTreesIndexes.next();
			List<Record<T>> result = new ArrayList<Record<T>>();
			Record<T> searchResult = treeIndex.search(queryRecord);
			if (searchResult != null)
				result.add(searchResult);
			return result.iterator();
		}
		return null;
	}
}
