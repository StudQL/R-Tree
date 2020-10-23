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
			Pair<Node<T>, Node<T>> splittedNodes = this.quadraticSplit(leaf, newNode);
			Node<T> L1 = splittedNodes.getFirst(), L2 = splittedNodes.getSecond();
			if (leaf == this.root)
				this.assignNewParent(L1, L2, null);
			else {
				Node<T> leafParent = leaf.getParent();
				leafParent.remove(leaf);
				leafParent.add(L1);
				leafParent.add(L2);
				this.adjustTree(L1, L2);
			}

		}
		System.out.println(this.toString());
		this.num_entries += 1;
	}

	public Node<T> chooseLeaf(Rectangle recordMbr, Node<T> R) {
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

	private Pair<Node<T>, Node<T>> quadraticSplit(Node<T> toSplitNode, Node<T> overflowNode) {
		// create a set of entries mbr
		ArrayList<Node<T>> records = new ArrayList<Node<T>>();
		for (Node<T> childRecord : toSplitNode.getChildren()) {
			records.add(childRecord);
		}
		records.add(overflowNode);
		// find the 2 nodes that maximizes the space waste, and assign them to a node
		Pair<Node<T>, Node<T>> seeds = this.pickSeeds(records);
		Node<T> L1 = new Node<T>(seeds.getFirst());
		Node<T> L2 = new Node<T>(seeds.getSecond());
		records.remove(seeds.getFirst());
		records.remove(seeds.getSecond());
		// examine remaining entries and add them to either L1 or L2 with the least
		// enlargement criteria
		int i = 0;
		while (i < records.size()) {
			// if one node must take all remaining entries, assign them with no criteria
			if (L1.numChildren() + records.size() == this.min_num_records) {
				L1.add(records);
				break;
			}
			if (L2.numChildren() + records.size() == this.min_num_records) {
				L2.add(records);
				break;
			}
			// add the next record to the node which will require the least enlargement
			this.pickNext(records, L1, L2);
			i++;
		}
		return new Pair<Node<T>, Node<T>>(L1, L2);

	}

	private Pair<Node<T>, Node<T>> pickSeeds(ArrayList<Node<T>> records) {
		float maxWaste = 0;
		Pair<Node<T>, Node<T>> wastefulePair = null;
		// iterate over all possible pairs
		for (int i = 0; i < records.size(); i++) {
			Node<T> E1 = records.get(i);
			for (int j = i + 1; j < records.size(); j++) {
				Node<T> E2 = records.get(j);
				// build rectangle that englobes E1 and E2
				Rectangle J = Rectangle.buildRectangle(E1.getMbr(), E2.getMbr());
//				System.out.println(J.toString());
				float d = J.area() - E1.getMbr().area() - E2.getMbr().area();
//				System.out.println(J.area());
//				System.out.println(E1.getMbr().area());
//				System.out.println(E2.getMbr().area());
				// chose most wasteful pair
				if (d > maxWaste) {
					maxWaste = d;
					wastefulePair = new Pair<Node<T>, Node<T>>(E1, E2);
				}
			}
		}
		return wastefulePair;
	}

	private void pickNext(ArrayList<Node<T>> records, Node<T> L1, Node<T> L2) {
		Node<T> chosenEntry = null;
		float maxDifference = 0;
		// get the max difference between area enlargments
		for (Node<T> entry : records) {
			Rectangle entryMbr = entry.getMbr();
			float enlargementL1 = L1.getMbr().calculateEnlargement(entryMbr);
			float enlargementL2 = L2.getMbr().calculateEnlargement(entryMbr);
			float maxEnlargementDifference = Math.abs(enlargementL1 - enlargementL2);
			if (maxEnlargementDifference >= maxDifference) {
				chosenEntry = entry;
				maxDifference = maxEnlargementDifference;
			}
		}
		// selecting group to which we add the selected entry
		this.resolveTies(L1, L2, chosenEntry);
		// remove chosenRecord from records
		records.remove(chosenEntry);
	}

	private void resolveTies(Node<T> L1, Node<T> L2, Node<T> chosenEntry) {
		float enlargementL1 = L1.getMbr().calculateEnlargement(chosenEntry.getMbr());
		float enlargementL2 = L2.getMbr().calculateEnlargement(chosenEntry.getMbr());
		if (enlargementL1 == enlargementL2) {
			// select group with min area
			float area1 = L1.getMbr().area();
			float area2 = L2.getMbr().area();
			if (area1 == area2) {
				int numEntries1 = L1.numChildren();
				int numEntries2 = L2.numChildren();
				// if it's still equal, resolve by default to L1
				if (numEntries1 <= numEntries2) {
					L1.add(chosenEntry);
				} else {
					L2.add(chosenEntry);
				}
			} else if (area1 < area1) {
				L1.add(chosenEntry);
			} else {
				L2.add(chosenEntry);
			}
		} else if (enlargementL1 < enlargementL2) {
			L1.add(chosenEntry);
		} else {
			L2.add(chosenEntry);
		}
	}

	public void adjustTree(Node<T> node, Node<T> createdNode) {
		Node<T> N = node;
		// node resulting from split
		Node<T> NN = createdNode;
		// while we do not reach root
		if (!N.isRoot()) {
			Node<T> P = N.getParent();
			// updating parent if there was no split
			P.updateMbr();
			// see if parent can accomodate NN
			if (NN != null && P.numChildren() > this.max_num_records) {
				P.remove(NN);
				Pair<Node<T>, Node<T>> splittedParents = this.quadraticSplit(P, NN);
				Node<T> splittedP = splittedParents.getFirst(), PP = splittedParents.getSecond();
				if (P == this.root)
					this.assignNewParent(splittedP, PP, null);
				else {
					Node<T> splittedParent = P.getParent();
					splittedParent.remove(P);
					splittedParent.add(splittedP);
					splittedParent.add(PP);
					this.adjustTree(splittedP, PP);
				}
			}
		}
	}

	public boolean delete(Record<T> record) {
		return false;
	}

	Record<T> search(Record<T> record) {
		return null;
	}

	List<Record<T>> rangeSearch(Rectangle r) {
		return null;
	}

	private void assignNewParent(Node<T> child1, Node<T> child2, Node<T> parent) {
		// reassign root
		if (parent == null) {
			Node<T> newRoot = new Node<T>();
			newRoot.add(child1);
			newRoot.add(child2);
			this.root = newRoot;
		} else {
			parent.add(child1);
			parent.add(child2);
		}
	}

	public String toString() {
		if (this.root == null)
			return "Empty R-Tree";
		return this.root.toString("");
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
