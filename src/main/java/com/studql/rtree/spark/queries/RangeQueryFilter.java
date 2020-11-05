package com.studql.rtree.spark.queries;

import org.apache.spark.api.java.function.Function;

import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Record;

public class RangeQueryFilter<T extends Boundable> implements Function<Record<T>, Boolean> {

	private static final long serialVersionUID = -5439468703603693237L;
	private Rectangle queryRange;

	public RangeQueryFilter(Rectangle queryRange) {
		this.queryRange = queryRange;
	}

	public Boolean call(Record<T> record) throws Exception {
		return queryRange.contains(record.getMbr());
	}
}
