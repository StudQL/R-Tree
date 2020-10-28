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

	public Node<T> getRoot() {
		return this.root;
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
			this.splitNodeAndReassign(leaf, newNode);
		}
//		System.out.println(this.toString());
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

	private void adjustTree(Node<T> node, Node<T> createdNode) {
		Node<T> previousNode = node;
		// node resulting from split
		Node<T> splittedNode = createdNode;
		// while we do not reach root
		if (!previousNode.isRoot()) {
			Node<T> previousParent = previousNode.getParent();
			// updating parent recursively in the no-split case
			if (splittedNode == null) {
				previousParent.updateMbr(previousNode.getMbr());
				this.adjustTree(previousParent, splittedNode);
			}
			// see if there is a node overflow, and update accordingly
			else if (previousParent.numChildren() > this.max_num_records) {
				previousParent.remove(splittedNode);
				this.splitNodeAndReassign(previousParent, splittedNode);
			}
		}
	}

	private void splitNodeAndReassign(Node<T> nodeToSplit, Node<T> overflowNode) {
		Pair<Node<T>, Node<T>> splittedNodes = this.splitter.splitNodes(nodeToSplit, overflowNode);
		Node<T> splittedLeft = splittedNodes.getFirst(), splittedRight = splittedNodes.getSecond();
		if (nodeToSplit == this.root)
			this.assignNewRoot(splittedLeft, splittedRight);
		else {
			Node<T> splittedParent = nodeToSplit.getParent();
			splittedParent.remove(nodeToSplit);
			splittedParent.add(splittedLeft);
			splittedParent.add(splittedRight);
			this.adjustTree(splittedLeft, splittedRight);
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
		Node<T> leaf = this.findLeafAndRemove(record, this.root);
		if (leaf == null)
			return false;
		this.condenseTree(leaf);
		// if root needs to be reassigned
		if (this.root.numChildren() == 1 && !this.root.isLeaf()) {
			Node<T> newRoot = this.root.getChildren().get(0);
			newRoot.setParent(null);
			this.root = newRoot;
		}
		this.num_entries -= 1;
		return true;
	}

	private Node<T> findLeafAndRemove(Record<T> record, Node<T> node) {
		if (!node.isLeaf()) {
			// perform DFS of child nodes
			for (Node<T> child : node.getChildren()) {
				if (child.getMbr().contains(record.getMbr())) {
					Node<T> foundLeaf = this.findLeafAndRemove(record, child);
					if (foundLeaf != null) {
						return foundLeaf;
					}
				}
			}
		} else {
			// node is leaf, checking all its records
			for (Node<T> child : node.getChildren()) {
				Record<T> childRecord = child.getRecord();
				if (childRecord.equals(record)) {
					node.remove(child);
					return node;
				}
			}
		}
		return null;
	}

	private void condenseTree(Node<T> leaf) {
		Node<T> N = leaf;
		ArrayList<Node<T>> removedEntries = new ArrayList<Node<T>>();
		if (!N.isRoot()) {
			Node<T> P = N.getParent();
			// N has underflow of childs
			if (N.numChildren() < this.min_num_records) {
				P.remove(N);
				// we will reinsert remaining entries if they have at least one child
				if (N.numChildren() > 0)
					removedEntries.add(N);
			} else {
				N.updateMbr(null);
			}
			// update parent recursively
			this.condenseTree(P);
		}
		// reinsert temporarily deleted entries
		for (Node<T> deletedChild : removedEntries) {
			this.insertFromNode(deletedChild);
		}
	}

	private void insertFromNode(Node<T> node) {
		if (node.getChildren() != null) {
			for (Node<T> child : node.getChildren()) {
				if (child.getRecord() != null)
					this.insert(child.getRecord());
				else
					this.insertFromNode(child);
			}
		}
	}

	public ArrayList<Record<T>> search(Node<T> R, Record<T> record) {
		Rectangle rec = record.getMbr();
		ArrayList<Record<T>> result = new ArrayList<Record<T>>();
		if (!R.isLeaf()) {
			List<Node<T>> temp = R.getChildren();
			for (Node<T> n : temp) {
				if (n.getMbr().isOverLapping(rec))
					result.addAll(rangeSearch(n, rec));
			}
		} else {
			List<Node<T>> temp = R.getChildren();
			for (Node<T> n : temp) {
				if (record.equals(n.getRecord()))
					result.add(n.getRecord());
			}
		}
		return result;
	}

	public ArrayList<Record<T>> rangeSearch(Node<T> R, Rectangle rec) {
		ArrayList<Record<T>> result = new ArrayList<Record<T>>();
		if (!R.isLeaf()) {
			List<Node<T>> temp = R.getChildren();
			for (Node<T> n : temp) {
				if (n.getMbr().isOverLapping(rec))
					result.addAll(rangeSearch(n, rec));
			}
		} else {
			List<Node<T>> temp = R.getChildren();
			for (Node<T> n : temp) {
				if (rec.contains(n.getMbr()))
					result.add(n.getRecord());
			}
		}
		return result;
	}

	public String toString() {
		if (this.root == null)
			return "Empty R-Tree";
		return this.root.toString("");
	}

	public int calculateHeight() {
		int height = 0;
		if (this.root == null) {
			return height;
		} else {
			height += 1;
			Node<T> R = this.root;
			while (!R.isLeaf()) {
				height += 1;
				R = R.getChildren().get(0);
			}
			height += 1;
		}
		return height;
	}
}
