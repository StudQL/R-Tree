package com.studql.rtree.spark.queries;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.studql.shape.Boundable;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class KnnQueryFilter<T extends Boundable>
		implements FlatMapFunction<Iterator<Record<T>>, Pair<Record<T>, Float>> {

	private static final long serialVersionUID = -4830349780359669211L;
	private int k;
	private Record<T> queryCenter;

	public KnnQueryFilter(int k, Record<T> queryCenter) {
		this.k = k;
		this.queryCenter = queryCenter;
	}

	// implement KNN for Iterator of records
	@Override
	public Iterator<Pair<Record<T>, Float>> call(Iterator<Record<T>> records) {
		Queue<Pair<Record<T>, Float>> knnQueue = new PriorityQueue<Pair<Record<T>, Float>>(k,
				new KnnDistanceComparator<T>());
		while (records.hasNext()) {
			Record<T> currentRecord = records.next();
			// fill the queue to get k records
			if (knnQueue.size() < k) {
				float distance = currentRecord.getMbr().distance(queryCenter.getMbr());
				knnQueue.offer(new Pair<Record<T>, Float>(currentRecord, distance));
			}
			// compare with farthest record of queue
			else {
				float currentDistance = currentRecord.getMbr().distance(queryCenter.getMbr());
				float farthestDistance = knnQueue.peek().getSecond();
				if (farthestDistance > currentDistance) {
					knnQueue.poll();
					knnQueue.offer(new Pair<Record<T>, Float>(currentRecord, currentDistance));
				}
			}
		}
		return knnQueue.iterator();
	}
}
