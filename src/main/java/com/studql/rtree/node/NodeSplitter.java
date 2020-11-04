package com.studql.rtree.node;

import java.io.Serializable;
import java.util.ArrayList;

import com.studql.shape.Boundable;
import com.studql.utils.Pair;

public abstract class NodeSplitter<T extends Boundable> implements Serializable {
	private static final long serialVersionUID = -354860141461294328L;

	abstract public Pair<Node<T>, Node<T>> splitNodes(Node<T> nodeToSplit, Node<T> overflowNode);

	abstract protected Pair<Node<T>, Node<T>> pickSeeds(ArrayList<Node<T>> records);

	abstract protected void pickNext(ArrayList<Node<T>> records, Node<T> L1, Node<T> L2);
}
