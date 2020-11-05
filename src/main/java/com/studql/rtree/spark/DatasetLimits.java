package com.studql.rtree.spark;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Record;

public class DatasetLimits<T extends Boundable> implements Serializable {
	private static final long serialVersionUID = 6064217564938193164L;
	private int record_count;
	private Rectangle mbr;

	public DatasetLimits(int record_count, Rectangle mbr) {
		this.record_count = record_count;
		this.mbr = mbr;
	}

	// merge two dataset limits objects (merge phase in aggregation)
	public static <T extends Boundable> DatasetLimits<T> add(DatasetLimits<T> currentLimits, Record<T> newRecord) {
		return merge(currentLimits, new DatasetLimits<T>(1, newRecord.getMbr()));
	}

	// add two dataset limits objects (add phase in aggregation)
	public static <T extends Boundable> DatasetLimits<T> merge(DatasetLimits<T> limits1, DatasetLimits<T> limits2) {
		if (limits1 == null)
			return limits2;
		if (limits2 == null)
			return limits1;
		int c1 = limits1.getRecordCount(), c2 = limits2.getRecordCount();
		Rectangle mbr1 = limits1.getMbr(), mbr2 = limits2.getMbr();
		return new DatasetLimits<T>(c1 + c2, Rectangle.buildRectangle(mbr1, mbr2));
	}

	public static <T extends Boundable> Function2<DatasetLimits<T>, Record<T>, DatasetLimits<T>> getAddFunction() {
		return new Function2<DatasetLimits<T>, Record<T>, DatasetLimits<T>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public DatasetLimits<T> call(DatasetLimits<T> limits, Record<T> record) throws Exception {
				return DatasetLimits.add(limits, record);
			}
		};
	}

	public static <T extends Boundable> Function2<DatasetLimits<T>, DatasetLimits<T>, DatasetLimits<T>> getMergeFunction() {
		return new Function2<DatasetLimits<T>, DatasetLimits<T>, DatasetLimits<T>>() {
			private static final long serialVersionUID = 2L;

			@Override
			public DatasetLimits<T> call(DatasetLimits<T> limits1, DatasetLimits<T> limits2) throws Exception {
				return DatasetLimits.merge(limits1, limits2);
			}
		};
	}

	public int getRecordCount() {
		return this.record_count;
	}

	public Rectangle getMbr() {
		return this.mbr;
	}
}
