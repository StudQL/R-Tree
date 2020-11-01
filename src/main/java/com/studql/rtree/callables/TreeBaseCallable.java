package src.main.java.com.studql.rtree.callables;

import src.main.java.com.studql.rtree.RtreeMulti;
import src.main.java.com.studql.shape.Boundable;

public abstract class TreeBaseCallable<T extends Boundable> {
	protected RtreeMulti<T> tree;
	protected int id;

	protected TreeBaseCallable(RtreeMulti<T> tree, int id) {
		this.tree = tree;
		this.id = id;
	}
}
