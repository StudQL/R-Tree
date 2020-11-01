package src.main.java.com.studql.rtree.node;

import java.util.ArrayList;

import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.utils.Pair;

public abstract class NodeSplitter<T extends Boundable> {
	abstract public Pair<Node<T>, Node<T>> splitNodes(Node<T> nodeToSplit, Node<T> overflowNode);

	abstract protected Pair<Node<T>, Node<T>> pickSeeds(ArrayList<Node<T>> records);

	abstract protected void pickNext(ArrayList<Node<T>> records, Node<T> L1, Node<T> L2);
}
