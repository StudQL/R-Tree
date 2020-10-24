package src.main.java.com.studql.rtree;

import java.util.ArrayList;

import src.main.java.com.studql.shape.Boundable;

public abstract class NodeSplitter<T extends Boundable> {
	abstract public Pair<Node<T>, Node<T>> splitNodes(Node<T> nodeToSplit, Node<T> overflowNode);

	abstract protected Pair<Node<T>, Node<T>> pickSeeds(ArrayList<Node<T>> records);

	abstract protected void pickNext(ArrayList<Node<T>> records, Node<T> L1, Node<T> L2);
}
