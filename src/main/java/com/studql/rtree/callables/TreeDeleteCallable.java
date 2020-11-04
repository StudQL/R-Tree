package com.studql.rtree.callables;

import java.util.List;
import java.util.concurrent.Callable;

import com.studql.rtree.RtreeMulti;
import com.studql.shape.Boundable;
import com.studql.utils.Record;

public class TreeDeleteCallable<T extends Boundable> extends TreeBaseCallable<T> implements Callable<List<Boolean>> {
	private List<Record<T>> records;

	public TreeDeleteCallable(RtreeMulti<T> tree, int id, List<Record<T>> records) {
		super(tree, id);
		this.records = records;
	}

	public List<Boolean> call() throws Exception {
		return tree.delete(records);
	}
}
