package com.studql.rtree.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;

import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Record;

import scala.Tuple2;

public class GridPartitioner<T extends Boundable> extends Partitioner {

	private static final long serialVersionUID = -4545594992467087350L;
	private List<Rectangle> spaceGrids;

	public GridPartitioner(List<Rectangle> spaceGrids) {
		this.spaceGrids = spaceGrids;
	}

	public <U extends Boundable> Iterator<Tuple2<Integer, Record<T>>> assignRecord(Record<T> record) {
		List<Tuple2<Integer, Record<T>>> result = new ArrayList<Tuple2<Integer, Record<T>>>();
		boolean wasAssigned = false;
		for (int i = 0; i < spaceGrids.size(); ++i) {
			Rectangle grid = spaceGrids.get(i);
			// grid fully contains record
			if (grid.contains(record.getMbr())) {
				result.add(new Tuple2<Integer, Record<T>>(i, record));
				wasAssigned = true;
			} else if (grid.isOverLapping(record.getMbr())) {
				// add to result without marking it assigned
				result.add(new Tuple2<>(i, record));
			}
		}
		// if no grid covers the record (or it only itersects partly), add it to the
		// overflow grid
		if (!wasAssigned)
			result.add(new Tuple2<Integer, Record<T>>(spaceGrids.size(), record));
		return result.iterator();
	}

	@Override
	public int numPartitions() {
		// 1 partition contains overflow records
		return spaceGrids.size() + 1;
	}

	@Override
	public int getPartition(Object id) {
		// partition each grid by its id
		return (int) id;
	}
}
