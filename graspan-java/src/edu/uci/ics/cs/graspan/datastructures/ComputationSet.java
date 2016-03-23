package edu.uci.ics.cs.graspan.datastructures;

/**
 * 
 * @author aftab
 * 
 *         Created Mar 20, 2016
 * 
 */
public class ComputationSet {

	// computation set

	// vertex ids
	private int oldTgts[] = null;
	private int newTgts[] = null;
	private int compOP[] = null;

	// edge vals
	private byte oldTgtEdgeVals[] = null;
	private byte newTgtEdgeVals[] = null;
	private byte compOpEdgeVals[] = null;

	public byte[] getOldTgtEdgeVals() {
		return oldTgtEdgeVals;
	}

	public void setOldTgtEdgeVals(byte[] oldTgtEdgeVals) {
		this.oldTgtEdgeVals = oldTgtEdgeVals;
	}

	public byte[] getNewTgtEdgeVals() {
		return newTgtEdgeVals;
	}

	public void setNewTgtEdgeVals(byte[] newTgtEdgeVals) {
		this.newTgtEdgeVals = newTgtEdgeVals;
	}

	public byte[] getOpEdgeVals() {
		return compOpEdgeVals;
	}

	public void setOpEdgeVals(byte[] compOpEdgeVals) {
		this.compOpEdgeVals = compOpEdgeVals;
	}

	public int[] getOldTgts() {
		return oldTgts;
	}

	public void setOldTgts(int[] oldTgts) {
		this.oldTgts = oldTgts;
	}

	public int[] getNewTgts() {
		return newTgts;
	}

	public void setNewTgts(int[] newTgts) {
		this.newTgts = newTgts;
	}

	public int[] getOp() {
		return compOP;
	}

	public void setOp(int[] compOP) {
		this.compOP = compOP;
	}
}
