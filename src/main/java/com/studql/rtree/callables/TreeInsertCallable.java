package com.studql.rtree.callables;

import java.util.List;
import java.util.concurrent.Callable;

import com.studql.rtree.RtreeMulti;
import com.studql.shape.Boundable;
import com.studql.utils.Record;

public class TreeInsertCallable<T extends Boundable> extends TreeBaseCallable<T> implements Callable<Void> {
	private List<Record<T>> records;

	public TreeInsertCallable(RtreeMulti<T> tree, int id, List<Record<T>> records) {
		super(tree, id);
		this.records = records;
		this.id = id;
	}

	@Override
	public Void call() throws Exception {
		tree.insert(records);
		return null;
	}
}
