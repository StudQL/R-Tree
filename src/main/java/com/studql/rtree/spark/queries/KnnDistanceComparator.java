package com.studql.rtree.spark.queries;

import java.io.Serializable;
import java.util.Comparator;

import com.studql.shape.Boundable;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class KnnDistanceComparator<T extends Boundable> implements Comparator<Pair<Record<T>, Float>>, Serializable {

	private static final long serialVersionUID = 8538255496374957399L;

	@Override
	public int compare(Pair<Record<T>, Float> r1, Pair<Record<T>, Float> r2) {
		return Float.compare(r1.getSecond(), r2.getSecond());
	}

}
