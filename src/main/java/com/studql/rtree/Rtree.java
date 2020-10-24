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
	private NodeSplitter<T> splitter;

	public Rtree() {
		this.num_entries = 0;
		this.min_num_records = DEFAULT_MIN_CHILDREN;
		this.max_num_records = DEFAULT_MAX_CHILDREN;
		this.root = new Node<T>();
		this.splitter = new QuadraticSplitter<T>(DEFAULT_MIN_CHILDREN);
	}

	public Rtree(int min_num_records, int max_num_records, NodeSplitter<T> splitter) {
		this.num_entries = 0;
		this.min_num_records = min_num_records;
		this.max_num_records = max_num_records;
		this.root = new Node<T>();
		this.splitter = splitter;
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

	public void insert(Record<T> record) {
		Rectangle recordMbr = record.getMbr();
		// choose leaf that needs the least enlargement with mbr
		Node<T> leaf = this.chooseLeaf(recordMbr, this.root);
		Node<T> newNode = new Node<T>(record);
		// if node has enough space to insert the child
		if (leaf.numChildren() < this.max_num_records) {
			leaf.add(newNode);
			this.adjustTree(leaf, null, true);
		} else {
			this.splitNodeAndReassign(leaf, newNode, true);
		}
		System.out.println(this.toString());
		this.num_entries += 1;
	}

	private Node<T> chooseLeaf(Rectangle recordMbr, Node<T> R) {
		if (R.isLeaf())
			return R;
		float minEnlargement = Float.MAX_VALUE;
		ArrayList<Node<T>> minEnlargedRecords = new ArrayList<Node<T>>();
		// choose record which mbr's enlarge the less with current record's mbr
		for (Node<T> child : R.getChildren()) {
			Rectangle childMbr = child.getMbr();
			float enlargement = childMbr.calculateEnlargement(recordMbr);
			if (enlargement == minEnlargement || minEnlargedRecords.size() == 0) {
				minEnlargedRecords.add(child);
				minEnlargement = enlargement;
			} else if (enlargement < minEnlargement) {
				minEnlargedRecords = new ArrayList<Node<T>>();
				minEnlargedRecords.add(child);
				minEnlargement = enlargement;
			}
		}
		if (minEnlargedRecords.size() == 1)
			return this.chooseLeaf(recordMbr, minEnlargedRecords.get(0));
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

	private void adjustTree(Node<T> node, Node<T> createdNode, boolean isAddOperation) {
		Node<T> previousNode = node;
		// node resulting from split
		Node<T> splittedNode = createdNode;
		// while we do not reach root
		if (!previousNode.isRoot()) {
			Node<T> previousParent = previousNode.getParent();
			// updating parent recursively in the no-split case
			if (splittedNode == null) {
				previousParent.updateMbr(previousNode.getMbr(), isAddOperation);
				this.adjustTree(previousParent, splittedNode, isAddOperation);
			}
			// see if there is a node overflow, and update accrodingly
			else if (previousParent.numChildren() > this.max_num_records) {
				previousParent.remove(splittedNode);
				this.splitNodeAndReassign(previousParent, splittedNode, isAddOperation);
			}
		}
	}

	private void splitNodeAndReassign(Node<T> nodeToSplit, Node<T> overflowNode, boolean isAddOperation) {
		Pair<Node<T>, Node<T>> splittedNodes = this.splitter.splitNodes(nodeToSplit, overflowNode);
		Node<T> splittedLeft = splittedNodes.getFirst(), splittedRight = splittedNodes.getSecond();
		if (nodeToSplit == this.root)
			this.assignNewRoot(splittedLeft, splittedRight);
		else {
			Node<T> splittedParent = nodeToSplit.getParent();
			splittedParent.remove(nodeToSplit);
			splittedParent.add(splittedLeft);
			splittedParent.add(splittedRight);
			this.adjustTree(splittedLeft, splittedRight, isAddOperation);
		}
	}

	private void assignNewRoot(Node<T> child1, Node<T> child2) {
		Node<T> newRoot = new Node<T>();
		newRoot.add(child1);
		newRoot.add(child2);
		this.root = newRoot;
	}

	public boolean delete(Record<T> record) {
		// choose leaf that contains record

		this.num_entries -= 1;
		return true;
	}

	Record<T> search(Record<T> record) {
		return null;
	}

	List<Record<T>> rangeSearch(Rectangle r) {
		return null;
	}

	public String toString() {
		if (this.root == null)
			return "Empty R-Tree";
		return this.root.toString("");
	}

	public int calculateHeight() {
		return 0;
	}
}
