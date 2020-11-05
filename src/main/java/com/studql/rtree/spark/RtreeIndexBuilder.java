package com.studql.rtree.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.rtree.Rtree;
import com.studql.rtree.node.NodeSplitter;
import com.studql.shape.Boundable;
import com.studql.utils.Record;

public class RtreeIndexBuilder<T extends Boundable> implements FlatMapFunction<Iterator<Record<T>>, Rtree<T>> {

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
	public Iterator<Rtree<T>> call(Iterator<Record<T>> shapeIterator) throws Exception {
		Rtree<T> rTreeIndex = new Rtree<T>(min_num_records, max_num_records, splitter);
		while (shapeIterator.hasNext()) {
			Record<T> record = shapeIterator.next();
			rTreeIndex.insert(record);
		}
		List<Rtree<T>> result = new ArrayList<Rtree<T>>();
		result.add(rTreeIndex);
		return result.iterator();
	}
}
