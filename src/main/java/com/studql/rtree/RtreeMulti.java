package com.studql.rtree;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.studql.rtree.node.Node;
import com.studql.rtree.node.NodeDistanceComparator;
import com.studql.rtree.node.NodeSplitter;
import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Pair;
import com.studql.utils.Record;

public class RtreeMulti<T extends Boundable> extends Rtree<T> {
	private Lock rootLock = new ReentrantLock();

	public RtreeMulti() {
		super();
	}

	public RtreeMulti(int min_num_records, int max_num_records, NodeSplitter<T> splitter) {
		super(min_num_records, max_num_records, splitter);
	}

	public void insert(Record<T> record) {
		rootLock.lock();
		try {
			super.insert(record);
		} finally {
			rootLock.unlock();
		}
	}

	public boolean delete(Record<T> record) {
		// choose leaf that contains record
		rootLock.lock();
		try {
			return super.delete(record);
		} finally {
			rootLock.unlock();
		}
	}

	public Record<T> search(Record<T> record) {
		this.root.getReadLock().lock();
		Rectangle recordMbr = record.getMbr();
		Record<T> result = null;
		// init stack for dfs in valid childs
		Stack<Node<T>> validNodes = new Stack<Node<T>>();
		validNodes.push(this.root);
		// traverse whole tree
		while (!validNodes.empty() && result == null) {
			Node<T> currentNode = validNodes.pop();
			for (Node<T> child : currentNode.getChildren()) {
				// record node
				Record<T> childRecord = child.getRecord();
				if (childRecord != null) {
					if (childRecord.equals(record)) {
						result = childRecord;
						break;
					}
				} else if (child.getMbr().contains(recordMbr)) {
					child.getReadLock().lock();
					validNodes.push(child);
				}
			}
			currentNode.getReadLock().unlock();
		}
		while (!validNodes.empty()) {
			validNodes.pop().getReadLock().unlock();
		}
		return result;
	}

	public List<Record<T>> rangeSearch(Rectangle rec) {
		this.root.getReadLock().lock();
		ArrayList<Record<T>> result = new ArrayList<Record<T>>();
		// init stack for dfs in valid childs
		Stack<Node<T>> validNodes = new Stack<Node<T>>();
		// traverse whole tree
		validNodes.push(this.root);
		while (!validNodes.empty()) {
			Node<T> currentNode = validNodes.pop();
			for (Node<T> child : currentNode.getChildren()) {
				// record node
				Record<T> childRecord = child.getRecord();
				if (childRecord != null) {
					if (rec.contains(childRecord.getMbr()))
						result.add(childRecord);
				} else if (child.getMbr().isOverLapping(rec)) {
					child.getReadLock().lock();
					validNodes.push(child);
				}
			}
			currentNode.getReadLock().unlock();
		}
		while (!validNodes.empty()) {
			validNodes.pop().getReadLock().unlock();
		}
		return result;
	}

	public List<Pair<Record<T>, Float>> nearestNeighborsSearch(Record<T> record, int k) {
		this.root.getReadLock().lock();
		ArrayList<Pair<Record<T>, Float>> result = new ArrayList<Pair<Record<T>, Float>>();
		Rectangle recordMbr = record.getMbr();
		// init stack for dfs in valid childs
		Queue<Node<T>> closestNodes = new PriorityQueue<Node<T>>(new NodeDistanceComparator<T>(recordMbr));
		// traverse whole tree
		closestNodes.add(this.root);
		while (!closestNodes.isEmpty() && k > 0) {
			Node<T> currentNode = closestNodes.poll();
			// node record
			if (currentNode.getRecord() != null) {
				if (!closestNodes.isEmpty() && currentNode.getMbr().distance(recordMbr) > closestNodes.peek().getMbr()
						.distance(recordMbr)) {
					currentNode.getReadLock().lock();
					closestNodes.add(currentNode);
				} else {
					result.add(new Pair<Record<T>, Float>(currentNode.getRecord(),
							currentNode.getMbr().distance(recordMbr)));
					--k;
				}
			} else {
				for (Node<T> child : currentNode.getChildren()) {
					child.getReadLock().lock();
					closestNodes.add(child);
				}
			}
			currentNode.getReadLock().unlock();
		}
		while (!closestNodes.isEmpty()) {
			closestNodes.poll().getReadLock().unlock();
		}
		return result;
	}

}
