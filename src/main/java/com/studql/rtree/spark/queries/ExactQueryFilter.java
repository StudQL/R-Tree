package com.studql.rtree.spark.queries;

import org.apache.spark.api.java.function.Function;

import com.studql.shape.Boundable;
import com.studql.utils.Record;

public class ExactQueryFilter<T extends Boundable> implements Function<Record<T>, Boolean> {

	private static final long serialVersionUID = 940231746548033725L;
	private Record<T> queryRecord;

	public ExactQueryFilter(Record<T> queryRecord) {
		this.queryRecord = queryRecord;
	}

	public Boolean call(Record<T> record) throws Exception {
		return record.equals(queryRecord);
	}
}
