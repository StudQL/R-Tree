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
		Node<T> leaf = this.chooseLeaf(recordMbr, this.root);
		boolean shouldSplit = false;
		if (leaf.numChildren() < this.max_num_records) {
			Node<T> newNode = new Node<T>(record);
			leaf.add(newNode);
		} else {
			shouldSplit = true;
			this.quadraticSplit(leaf);
		}
		this.num_entries += 1;
		this.adjustTree(leaf, shouldSplit);
	}

	public Node<T> chooseLeaf(Rectangle recordMbr, Node<T> R) {
		if (R.isLeaf())
			return R;
		float minEnlargement = Float.MAX_VALUE;
		List<Node<T>> minEnlargedRecords = new ArrayList<Node<T>>();
		// choose record which mbr's enlarge the less with current record's mbr
		for (Node<T> node : R.getChildren()) {
			Rectangle currentRectangle = node.getMbr();
			float enlargement = currentRectangle.calculateEnlargement(recordMbr);
			if (enlargement <= minEnlargement) {
				minEnlargement = enlargement;
				minEnlargedRecords.add(node);
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

	public void quadraticSplit(Node<T> node) {

	}

	public void adjustTree(Node<T> node, boolean hasSplit) {
		if (!hasSplit) {
			node.updateMbr();
			while (node.getParent() != null) {
				node = node.getParent();
				node.updateMbr();
			}
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
