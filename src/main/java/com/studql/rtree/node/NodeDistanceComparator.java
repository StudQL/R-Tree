package com.studql.rtree.node;

import java.util.Comparator;

import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;

public class NodeDistanceComparator<T extends Boundable> implements Comparator<Node<T>> {
	private Rectangle objectMbr;

	public NodeDistanceComparator(Rectangle objectMbr) {
		this.objectMbr = objectMbr;
	}

	public int compare(Node<T> n1, Node<T> n2) {
		float n1Distance = n1.getMbr().distance(objectMbr);
		float n2Distance = n2.getMbr().distance(objectMbr);
		return n1Distance < n2Distance ? -1 : n2Distance < n1Distance ? 1 : 0;
	}
}
