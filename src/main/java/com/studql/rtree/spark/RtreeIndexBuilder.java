package com.studql.rtree.spark;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.rtree.Rtree;
import com.studql.rtree.node.NodeSplitter;
import com.studql.shape.Boundable;
import com.studql.utils.Record;

public class RtreeIndexBuilder<T extends Boundable> implements FlatMapFunction<Iterator<T>, Rtree<T>> {

	private static final long serialVersionUID = 557205903900832769L;
	private int min_num_records;
	private int max_num_records;
	private NodeSplitter<T> splitter;

	public RtreeIndexBuilder(int min_num_records, int max_num_records, NodeSplitter<T> splitter) {
		this.min_num_records = min_num_records;
		this.max_num_records = max_num_records;
		this.splitter = splitter;
	}

	@Override
	public Iterator<Rtree<T>> call(Iterator<T> shapeIterator) throws Exception {
		Rtree<T> rTreeIndex = new Rtree<T>(min_num_records, max_num_records, splitter);
		int i = 0;
		while (shapeIterator.hasNext()) {
			T shape = shapeIterator.next();
			rTreeIndex.insert(new Record<T>(shape, Integer.toString(i)));
			++i;
		}
		Set<Rtree<T>> result = new HashSet<Rtree<T>>();
		result.add(rTreeIndex);
		return result.iterator();
	}
}
