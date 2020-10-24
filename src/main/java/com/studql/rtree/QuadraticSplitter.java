package src.main.java.com.studql.rtree;

import java.util.ArrayList;

import src.main.java.com.studql.shape.Boundable;
import src.main.java.com.studql.shape.Rectangle;

public class QuadraticSplitter<T extends Boundable> extends NodeSplitter<T> {

	private int min_num_records;

	public QuadraticSplitter(int min_num_records) {
		this.min_num_records = min_num_records;
	}

	protected Pair<Node<T>, Node<T>> pickSeeds(ArrayList<Node<T>> records) {
		float maxWaste = 0;
		Pair<Node<T>, Node<T>> mostWastefulePair = null;
		// iterate over all possible pairs
		for (int i = 0; i < records.size(); i++) {
			Node<T> E1 = records.get(i);
			for (int j = i + 1; j < records.size(); j++) {
				Node<T> E2 = records.get(j);
				// build rectangle that englobes E1 and E2
				Rectangle enclosingRectangle = Rectangle.buildRectangle(E1.getMbr(), E2.getMbr());
				float d = enclosingRectangle.area() - E1.getMbr().area() - E2.getMbr().area();
				// chose most wasteful pair
				if (d >= maxWaste) {
					maxWaste = d;
					mostWastefulePair = new Pair<Node<T>, Node<T>>(E1, E2);
				}
			}
		}
		return mostWastefulePair;
	}

	protected void pickNext(ArrayList<Node<T>> records, Node<T> L1, Node<T> L2) {
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

	public Pair<Node<T>, Node<T>> splitNodes(Node<T> nodeToSplit, Node<T> overflowNode) {
		// create a set of entries mbr
		ArrayList<Node<T>> records = new ArrayList<Node<T>>();
		for (Node<T> childRecord : nodeToSplit.getChildren()) {
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
		while (records.size() > 0) {
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
		}
		return new Pair<Node<T>, Node<T>>(L1, L2);
	}
}
