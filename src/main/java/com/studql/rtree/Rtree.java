package src.main.java.com.studql.rtree;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

import src.main.java.com.studql.rtree.node.Node;
import src.main.java.com.studql.rtree.node.NodeDistanceComparator;
import src.main.java.com.studql.rtree.node.NodeSplitter;
import src.main.java.com.studql.rtree.node.QuadraticSplitter;
import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;
import src.main.java.com.studql.utils.Pair;
import src.main.java.com.studql.utils.Record;

public class Rtree<T extends Boundable> {
	private final static int DEFAULT_MIN_CHILDREN = 2;
	private final static int DEFAULT_MAX_CHILDREN = 4;

	protected Node<T> root;
	protected int num_entries;
	protected int min_num_records;
	protected int max_num_records;
	protected NodeSplitter<T> splitter;

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
		this.num_entries += 1;
	}

	public void insert(List<Record<T>> records) {
		for (Record<T> record : records) {
			this.insert(record);
		}
	}

	protected Node<T> chooseLeaf(Rectangle recordMbr, Node<T> R) {
		Node<T> current = this.root;
		while (!current.isLeaf()) {
			ArrayList<Node<T>> minEnlargedRecords = this.getMinEnlargedRecords(current, recordMbr);
			if (minEnlargedRecords.size() == 1)
				current = minEnlargedRecords.get(0);
			else {
				// resolve ties if any, by choosing the node with least mbr's area
				current = this.getMinAreaRecord(minEnlargedRecords);
			}
		}
		return current;
	}

	protected Node<T> getMinAreaRecord(ArrayList<Node<T>> nodes) {
		Node<T> minAreaRecord = null;
		float minArea = Float.MAX_VALUE;
		for (Node<T> node : nodes) {
			float area = node.getMbr().area();
			if (area < minArea) {
				minAreaRecord = node;
				minArea = area;
			}
		}
		return minAreaRecord;
	}

	protected ArrayList<Node<T>> getMinEnlargedRecords(Node<T> current, Rectangle recordMbr) {
		float minEnlargement = Float.MAX_VALUE;
		ArrayList<Node<T>> minEnlargedRecords = new ArrayList<Node<T>>();
		// choose record which mbr's enlarge the less with current record's mbr
		for (Node<T> child : current.getChildren()) {
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
		return minEnlargedRecords;
	}

	protected void adjustTree(Node<T> node, Node<T> createdNode) {
		Node<T> previousNode = node;
		// node resulting from split
		Node<T> splittedNode = createdNode;
		// while we do not reach root
		while (!previousNode.isRoot()) {
			Node<T> previousParent = previousNode.getParent();
			// updating parent recursively in the no-split case
			if (splittedNode == null) {
				previousParent.updateMbr(previousNode.getMbr());
			}
			// see if there is a node overflow, and update accordingly
			else if (previousParent.numChildren() > this.max_num_records) {
				previousParent.remove(splittedNode);
				this.splitNodeAndReassign(previousParent, splittedNode);
			}
			previousNode = previousParent;
		}
	}

	protected void splitNodeAndReassign(Node<T> nodeToSplit, Node<T> overflowNode) {
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

	protected void assignNewRoot(Node<T> child1, Node<T> child2) {
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

	public List<Boolean> delete(List<Record<T>> records) {
		List<Boolean> deletedRecords = new ArrayList<Boolean>();
		for (Record<T> record : records) {
			deletedRecords.add(this.delete(record));
		}
		return deletedRecords;
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

	public Record<T> search(Record<T> record) {
		Rectangle recordMbr = record.getMbr();
		Record<T> result = null;
		// init stack for dfs in valid childs
		Stack<Node<T>> validNodes = new Stack<Node<T>>();
		validNodes.push(this.root);
		// traverse whole tree
		while (!validNodes.empty() && result == null) {
			Node<T> currentNode = validNodes.pop();
			for (Node<T> child : currentNode.getChildren()) {
				// record node
				Record<T> childRecord = child.getRecord();
				if (childRecord != null) {
					if (childRecord.equals(record)) {
						result = childRecord;
						break;
					}
				} else if (child.getMbr().contains(recordMbr)) {
					validNodes.push(child);
				}
			}
		}
		return result;
	}

	public List<Record<T>> search(List<Record<T>> records) {
		List<Record<T>> searchResults = new ArrayList<Record<T>>();
		for (Record<T> searchRecord : records) {
			searchResults.add(this.search(searchRecord));
		}
		return searchResults;
	}

	public List<Record<T>> rangeSearch(Rectangle rec) {
		ArrayList<Record<T>> result = new ArrayList<Record<T>>();
		// init stack for dfs in valid childs
		Stack<Node<T>> validNodes = new Stack<Node<T>>();
		// traverse whole tree
		validNodes.push(this.root);
		while (!validNodes.empty()) {
			Node<T> currentNode = validNodes.pop();
			for (Node<T> child : currentNode.getChildren()) {
				// record node
				Record<T> childRecord = child.getRecord();
				if (childRecord != null) {
					if (rec.contains(childRecord.getMbr()))
						result.add(childRecord);
				} else if (child.getMbr().isOverLapping(rec)) {
					validNodes.push(child);
				}
			}
		}
		return result;
	}

	public List<List<Record<T>>> rangeSearch(List<Rectangle> rectangles) {
		List<List<Record<T>>> searchResults = new ArrayList<List<Record<T>>>();
		for (Rectangle r : rectangles) {
			searchResults.add(this.rangeSearch(r));
		}
		return searchResults;
	}

	public List<Pair<Record<T>, Float>> nearestNeighborsSearch(Record<T> record, int k) {
		ArrayList<Pair<Record<T>, Float>> result = new ArrayList<Pair<Record<T>, Float>>();
		Rectangle recordMbr = record.getMbr();
		// init stack for dfs in valid childs
		Queue<Node<T>> closestNodes = new PriorityQueue<Node<T>>(new NodeDistanceComparator<T>(recordMbr));
		// traverse whole tree
		closestNodes.add(this.root);
		while (!closestNodes.isEmpty() && k > 0) {
			Node<T> currentNode = closestNodes.poll();
			// node record
			if (currentNode.getRecord() != null) {
				if (!closestNodes.isEmpty() && currentNode.getMbr().distance(recordMbr) > closestNodes.peek().getMbr()
						.distance(recordMbr)) {
					closestNodes.add(currentNode);
				} else {
					result.add(new Pair<Record<T>, Float>(currentNode.getRecord(),
							currentNode.getMbr().distance(recordMbr)));
					--k;
				}
			} else {
				for (Node<T> child : currentNode.getChildren()) {
					closestNodes.add(child);
				}
			}
		}
		return result;
	}

	public List<List<Pair<Record<T>, Float>>> nearestNeighborsSearch(List<Record<T>> records, int k) {
		List<List<Pair<Record<T>, Float>>> searchResults = new ArrayList<List<Pair<Record<T>, Float>>>();
		for (Record<T> r : records) {
			searchResults.add(this.nearestNeighborsSearch(r, k));
		}
		return searchResults;
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
