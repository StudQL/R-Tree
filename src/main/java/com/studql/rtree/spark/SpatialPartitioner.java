package com.studql.rtree.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

import com.studql.rtree.Rtree;
import com.studql.rtree.node.Node;
import com.studql.rtree.node.NodeSplitter;
import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Record;

public class SpatialPartitioner<T extends Boundable> implements Serializable {

	private static final long serialVersionUID = -4814281881052201781L;
	List<Rectangle> spaceGrids;

	public SpatialPartitioner(List<Record<T>> sampledRecords, int num_partitions, NodeSplitter<T> splitter,
			Function<Integer, Integer> minRecordFunc) {
		// build Rtree
		int max_num_records = sampledRecords.size() / num_partitions;
		int min_num_records = minRecordFunc.apply(max_num_records);
		Rtree<T> tree = new Rtree<T>(min_num_records, max_num_records, splitter);
		tree.insert(sampledRecords);
		// compute grid bounds
		this.spaceGrids = computeGridsFromTree(tree);
	}

	private List<Rectangle> computeGridsFromTree(Rtree<T> tree) {
		List<Rectangle> result = new ArrayList<Rectangle>();
		// traverse the tree and select all leaves MBR
		Stack<Node<T>> nodes = new Stack<Node<T>>();
		nodes.push(tree.getRoot());
		// traverse whole tree
		while (!nodes.empty()) {
			Node<T> currentNode = nodes.pop();
			// add leaf's MBR
			if (currentNode.isLeaf()) {
				result.add(currentNode.getMbr());
			} else if (currentNode.getChildren() != null) {
				for (Node<T> child : currentNode.getChildren()) {
					nodes.add(child);
				}
			}
		}
		return result;
	}

	public List<Rectangle> getSpaceGrids() {
		return this.spaceGrids;
	}
}
