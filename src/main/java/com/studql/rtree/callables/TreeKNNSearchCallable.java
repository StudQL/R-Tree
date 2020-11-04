package com.studql.rtree.callables;

import java.util.List;
import java.util.concurrent.Callable;

import com.studql.rtree.RtreeMulti;
import com.studql.shape.Boundable;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class TreeKNNSearchCallable<T extends Boundable> extends TreeBaseCallable<T>
		implements Callable<List<List<Pair<Record<T>, Float>>>> {
	private List<Record<T>> records;
	private int k;

	public TreeKNNSearchCallable(RtreeMulti<T> tree, int id, List<Record<T>> records, int k) {
		super(tree, id);
		this.records = records;
		this.k = k;
	}

	public List<List<Pair<Record<T>, Float>>> call() throws Exception {
		return tree.nearestNeighborsSearch(records, k);
	}
}
