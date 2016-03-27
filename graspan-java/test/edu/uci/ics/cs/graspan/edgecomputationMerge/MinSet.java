package edu.uci.ics.cs.graspan.edgecomputationMerge;

import java.util.HashSet;

public class MinSet {

	// always points to the minimum element
	private int ptr;

	private int minSetId;
	private int currentVId;
	private HashSet<Integer> evals;

	public int getMinSetId() {
		return minSetId;
	}

	public void setMinSetId(int id) {
		minSetId = id;
	}

	public MinSet() {
		ptr = -1;
		evals = new HashSet<Integer>();
	}

	public int getCurrentVId() {
		return currentVId;
	}

	public void setCurrentVId(int id) {
		currentVId = id;
	}

	public int getPtr() {
		return ptr;
	}

	public void incrementPtr() {
		ptr++;
	}

	public void addEval(int eval) {
		evals.add(eval);
	}

	public void clearEvalSet() {
		evals.clear();
	}

	public HashSet<Integer> getEvals() {
		return evals;
	}

	@Override
	public String toString() {
		String s = "";
		s = this.minSetId + ": "
				+ this.getCurrentVId() + " " + this.getEvals();
		return s;

	}

}
