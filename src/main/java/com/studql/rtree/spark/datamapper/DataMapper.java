package com.studql.rtree.spark.datamapper;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.shape.Boundable;
import com.studql.utils.Pair;

public abstract class DataMapper<T extends Boundable> implements FlatMapFunction<Iterator<String>, T> {

	private static final long serialVersionUID = 4965043700565916277L;
	protected String delimiter;
	protected int[] pointPositionInLine;
	protected Pair<float[], float[]> rangeInterpolators;
	protected boolean hasHeader;

	@Override
	public abstract Iterator<T> call(Iterator<String> t) throws Exception;
}
