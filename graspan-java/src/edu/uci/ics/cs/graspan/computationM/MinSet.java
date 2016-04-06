package edu.uci.ics.cs.graspan.computationM;

import java.util.HashSet;

public class MinSet {

	// always points to the minimum element
	private int ptr;

	private final int minSetId;
	
	private int currentVId;
	
	private HashSet<Byte> evals;

	public int getMinSetId() {
		return minSetId;
	}


	public MinSet(int minSetId) {
		ptr = 0;
		this.minSetId = minSetId;
		evals = new HashSet<Byte>();
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

	public void addEval(byte eval) {
		evals.add(eval);
	}

	public void clearEvalSet() {
		evals.clear();
	}

	public HashSet<Byte> getEvals() {
		return evals;
	}

	@Override
	public String toString() {
		String s = "";
		s = "MinSet no. " + this.minSetId + ": " + this.getCurrentVId() + " "
				+ this.getEvals();
		return s;

	}

}
