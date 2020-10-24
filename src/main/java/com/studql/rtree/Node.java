package src.main.java.com.studql.rtree;

import java.util.ArrayList;
import java.util.List;

import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;

public class Node<T extends Boundable> {
	private Rectangle mbr = null;
	private Node<T> parent = null;
	// nodes all have a list of child nodes, and it's the same for leaves, only
	// their child only have a record value
	private List<Node<T>> children = null;
	private Record<T> record = null;

	public Node() {
		this.children = new ArrayList<Node<T>>();
	}

	public Node(Node<T> child) {
		this.children = new ArrayList<Node<T>>();
		this.add(child);
		this.mbr = child.getMbr();
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
		if (this.children == null || this.children.size() == 0) {
			return true;
		}
		Node<T> leafTest = this.children.get(0);
		// if child has a record then the instance it's parent is a leaf
		if (leafTest.getRecord() != null) {
			return true;
		}
		return false;
	}

	public boolean isRoot() {
		return this.parent == null;
	}

	public void add(Node<T> node) {
		this.children.add(node);
		node.setParent(this);
		this.updateMbr(node.getMbr(), true);
	}

	public void add(List<Node<T>> nodes) {
		for (Node<T> node : nodes) {
			this.add(node);
		}
	}

	public void remove(Node<T> node) {
		this.children.remove(node);
		node.setParent(null);
		this.updateMbr(node.getMbr(), false);
	}

	public String toString(String padding) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(padding + "Node(");
		if (this.isLeaf() && this.record != null)
			strBuilder.append("record=" + this.record.toString() + ")");
		else {
			if (this.mbr != null) {
				strBuilder.append("mbr=" + this.mbr.toString() + ")\n");
			}
			for (Node<T> child : this.children) {
				strBuilder.append(child.toString(padding + "  "));
			}
		}
		strBuilder.append("\n");
		return strBuilder.toString();
	}

	public void updateMbr(Rectangle childMbrChange, boolean isAddOperation) {
		// if there is already a minimum bounding rectangle
		if (this.mbr != null) {
			Rectangle enclosing = null;
			if (isAddOperation)
				enclosing = Rectangle.buildRectangle(this.mbr, childMbrChange);
			else {
				// traverse all childs
				for (Node<T> child : this.children) {
					if (enclosing == null) {
						enclosing = child.getMbr();
					} else {
						enclosing = Rectangle.buildRectangle(enclosing, child.getMbr());
					}
				}
			}
			if (enclosing != this.mbr) {
				this.setMbr(enclosing);
			}
		} else {
			this.setMbr(childMbrChange);
		}
	}
}
