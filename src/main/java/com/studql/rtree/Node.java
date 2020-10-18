package src.main.java.com.studql.rtree;

import java.util.ArrayList;
import java.util.List;

import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;

public class Node<T extends Boundable> {
	private Rectangle mbr = null;
	private Node<T> parent = null;
	private List<Node<T>> children = null;
	private Record<T> record = null;

	public Node() {
		this.children = new ArrayList<Node<T>>();
	}

	public Node(Record<T> record) {
		this.record = record;
		this.mbr = record.getMbr();
	}

	public Rectangle getMbr() {
		return this.mbr;
	}

	private void setMbr(Rectangle value) {
		this.mbr = value;
	}

	public Node<T> getParent() {
		return this.parent;
	}

	public void setParent(Node<T> value) {
		this.parent = value;
	}

	public List<Node<T>> getChildren() {
		return this.children;
	}

	public Record<T> getRecord() {
		return this.record;
	}

	public int numChildren() {
		if (this.children != null)
			return this.children.size();
		return 0;
	}

	public boolean isLeaf() {
		if (this.children == null || this.children.size() <= 1) {
			return true;
		}
		Node<T> leafTest = this.children.get(0);
		// if it has a record then it's parent is a leaf
		if (leafTest.getRecord() != null) {
			return true;
		}
		return false;
	}

	public void add(Node<T> node) {
		this.children.add(node);
		node.setParent(this);
	}

	public boolean delete(Node<T> node) {
		return this.children.remove(node);
	}

	public void updateMbr() {
		float minX = Float.MAX_VALUE;
		float minY = minX;
		float maxX = Float.MIN_VALUE;
		float maxY = maxX;
		for (Node<T> child : this.children) {
			Rectangle childMbr = child.getMbr();
			ArrayList<Float> mbrLimitDimensions = Rectangle.getMinMaxDimensions(childMbr);
			minX = Math.min(minX, mbrLimitDimensions.get(0));
			maxX = Math.max(maxX, mbrLimitDimensions.get(1));
			minY = Math.min(minY, mbrLimitDimensions.get(2));
			maxY = Math.max(maxY, mbrLimitDimensions.get(3));
		}
		Rectangle newMbr = new Rectangle(minX, maxY, maxX, minY);
		if (newMbr != this.mbr) {
			this.setMbr(newMbr);
		}
	}
}
