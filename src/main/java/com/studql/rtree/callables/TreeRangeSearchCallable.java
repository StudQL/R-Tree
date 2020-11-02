package src.main.java.com.studql.rtree.callables;

import java.util.List;
import java.util.concurrent.Callable;

import src.main.java.com.studql.rtree.RtreeMulti;
import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;
import src.main.java.com.studql.utils.Record;

public class TreeRangeSearchCallable<T extends Boundable> extends TreeBaseCallable<T>
		implements Callable<List<List<Record<T>>>> {
	private List<Rectangle> rangeRectangles;

	public TreeRangeSearchCallable(RtreeMulti<T> tree, int id, List<Rectangle> rangeRectangles) {
		super(tree, id);
		this.rangeRectangles = rangeRectangles;
	}

	public List<List<Record<T>>> call() throws Exception {
		return tree.rangeSearch(rangeRectangles);
	}
}
