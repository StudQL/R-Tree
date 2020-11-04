package com.studql.rtree.callables;

import com.studql.rtree.RtreeMulti;
import com.studql.shape.Boundable;

public abstract class TreeBaseCallable<T extends Boundable> {
	protected RtreeMulti<T> tree;
	protected int id;

	protected TreeBaseCallable(RtreeMulti<T> tree, int id) {
		this.tree = tree;
		this.id = id;
	}
}
