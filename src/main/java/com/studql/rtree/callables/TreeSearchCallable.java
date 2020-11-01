package src.main.java.com.studql.rtree.callables;

import java.util.List;
import java.util.concurrent.Callable;

import src.main.java.com.studql.rtree.RtreeMulti;
import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.utils.Record;

public class TreeSearchCallable<T extends Boundable> extends TreeBaseCallable<T> implements Callable<List<Record<T>>> {
	private List<Record<T>> records;

	public TreeSearchCallable(RtreeMulti<T> tree, int id, List<Record<T>> records) {
		super(tree, id);
		this.records = records;
	}

	public List<Record<T>> call() throws Exception {
		List<Record<T>> results = tree.search(records);
		return results;
	}
}
