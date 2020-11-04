package com.studql.rtree.node;

import java.util.ArrayList;

import com.studql.shape.Boundable;
import com.studql.shape.Rectangle;
import com.studql.utils.Pair;

public class LinearSplitter<T extends Boundable> extends NodeSplitter<T> {

	private int min_num_records;

	public LinearSplitter(int min_num_records) {
		this.min_num_records = min_num_records;
	}

	/*
	 * This method picks the 2 rectangles we use to make the 2 new nodes using the
	 * Linear Splitting method
	 */
	protected Pair<Node<T>, Node<T>> pickSeeds(ArrayList<Node<T>> records) {
		Pair<Node<T>, Node<T>> NewNodes = null;

		Node<T> E = records.get(0);
		Rectangle EMbr = E.getMbr();
		float lowest = EMbr.lowest();
		float highest = EMbr.highest();
		Node<T> EH = E, EL = E;
		for (int i = 1; i < records.size(); i++) {
			Node<T> Ei = records.get(i);
			Rectangle EMbri = Ei.getMbr();
			float lowxy = EMbri.lowest();
			float highxy = EMbri.highest();
			// check whether the current rectangle is higher or lower than the highest and
			// lowest
			// rectangle we have so far
			if (lowxy < lowest) {
				lowest = lowxy;
				EL = Ei;
			}
			if (highxy > highest) {
				highest = highxy;
				EH = Ei;
			}
		}
		NewNodes = new Pair<Node<T>, Node<T>>(EL, EH);

		// System.out.println(EH.getMbr().toString() + highest);
		return NewNodes;
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
