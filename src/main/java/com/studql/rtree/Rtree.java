package src.main.java.com.studql.rtree;

import java.util.ArrayList;
import java.util.List;

import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;

public final class Rtree<T extends Boundable> {
	private final static int DEFAULT_MIN_CHILDREN = 2;
	private final static int DEFAULT_MAX_CHILDREN = 4;

	private Node<T> root;
	private int num_entries;
	private int min_num_records;
	private int max_num_records;

	public Rtree() {
		this.num_entries = 0;
		this.min_num_records = DEFAULT_MIN_CHILDREN;
		this.max_num_records = DEFAULT_MAX_CHILDREN;
		this.root = new Node<T>();
	}

	public Rtree(int min_num_records, int max_num_records) {
		this.num_entries = 0;
		this.min_num_records = min_num_records;
		this.max_num_records = max_num_records;
		this.root = new Node<T>();
	}

	public void insert(Record<T> record) {
		Rectangle recordMbr = record.getMbr();
		// choose leaf that needs the least enlargement with mbr
		Node<T> leaf = this.chooseLeaf(recordMbr, this.root);
		Node<T> newNode = new Node<T>(record);
		// if node has enough space to insert the child
		if (leaf.numChildren() < this.max_num_records) {
			leaf.add(newNode);
			this.adjustTree(leaf, null);
		} else {
			Node<T> splitNode = this.quadraticSplit(leaf, newNode);
			splitNode.setParent(leaf.getParent());
			this.adjustTree(leaf, splitNode);
			// if the root was split, create new node
			if (leaf == this.root) {
				this.assignNewRoot(leaf, splitNode);
			}
		}
		this.num_entries += 1;
	}

	public Node<T> chooseLeaf(Rectangle recordMbr, Node<T> R) {
		if (R.isLeaf())
			return R;
		float minEnlargement = Float.MAX_VALUE;
		List<Node<T>> minEnlargedRecords = new ArrayList<Node<T>>();
		// choose record which mbr's enlarge the less with current record's mbr
		for (Node<T> child : R.getChildren()) {
			Rectangle childMbr = child.getMbr();
			float enlargement = childMbr.calculateEnlargement(recordMbr);
			if (enlargement <= minEnlargement) {
				minEnlargement = enlargement;
				minEnlargedRecords.add(child);
			}
		}
		// resolve ties if any, by choosing the node with least mbr's area
		Node<T> minAreaRecord = null;
		float minArea = Float.MAX_VALUE;
		for (Node<T> node : minEnlargedRecords) {
			float area = node.getMbr().area();
			if (area < minArea) {
				minAreaRecord = node;
				minArea = area;
			}
		}
		return this.chooseLeaf(recordMbr, minAreaRecord);
	}

	public Node<T> quadraticSplit(Node<T> node, Node<T> overflowNode) {
		return null;
	}

	public void adjustTree(Node<T> node, Node<T> createdNode) {
		Node<T> N = node;
		// node resulting from split
		Node<T> NN = createdNode;
		// while we do not reach root
		if (!N.isRoot()) {
			N.updateMbr();
			Node<T> P = N.getParent();
			// see if parent can accomodate NN
			if (P.numChildren() < this.max_num_records && NN != null) {
				P.add(NN);
			} else if (NN != null) {
				Node<T> PP = this.quadraticSplit(P, NN);
				NN.setParent(PP);
				NN = PP;
			}
			N = P;
		}

	}

	public void delete(Record<T> record) {

	}

	Record<T> search(Record<T> record) {
		return null;
	}

	List<Record<T>> rangeSearch(Rectangle r) {
		return null;
	}

	private void assignNewRoot(Node<T> child1, Node<T> child2) {
		Node<T> newRoot = new Node<T>();
		newRoot.add(child1);
		newRoot.add(child2);
		this.root = newRoot;
	}

	public String toString() {
		return "";
	}

	public int calculateHeight() {
		return 0;
	}

	public int getNumEntries() {
		return this.num_entries;
	}

	public int getMinRecords() {
		return this.min_num_records;
	}

	public int getMaxRecords() {
		return this.max_num_records;
	}
}
