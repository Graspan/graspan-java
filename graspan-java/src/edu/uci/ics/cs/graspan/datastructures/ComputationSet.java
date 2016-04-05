package edu.uci.ics.cs.graspan.datastructures;

/**
 * 
 * @author aftab
 * 
 *         Created Mar 20, 2016
 * 
 */
public class ComputationSet {

	// INPUT FOR EACH ITERATION

	// old

	private int oldEdgs[];
	private byte oldVals[];

	public int[] getOldEdgs() {
		return oldEdgs;
	}

	public void setOldEdgs(int[] oldEdgs) {
		this.oldEdgs = oldEdgs;
	}

	public byte[] getOldVals() {
		return oldVals;
	}

	public void setOldVals(byte[] oldVals) {
		this.oldVals = oldVals;
	}

	// new

	private int newEdgs[];
	private byte newVals[];

	public int[] getNewEdgs() {
		return newEdgs;
	}

	public void setNewEdgs(int[] newEdgs) {
		this.newEdgs = newEdgs;
	}

	public byte[] getNewVals() {
		return newVals;
	}

	public void setNewVals(byte[] newVals) {
		this.newVals = newVals;
	}

	// oldUnew

	private int oldUnewEdgs[];
	private byte oldUnewVals[];

	public int[] getOldUnewEdgs() {
		return oldUnewEdgs;
	}

	public void setOldUnewEdgs(int[] oldUnewEdgs) {
		this.oldUnewEdgs = oldUnewEdgs;
	}

	public byte[] getOldUnewVals() {
		return oldUnewVals;
	}

	public void setOldUnewVals(byte[] oldUnewVals) {
		this.oldUnewVals = oldUnewVals;
	}

	// OUPUT FOR EACH ITERATION

	// delta

	private int deltaEdgs[];
	private byte deltaVals[];

	public int[] getDeltaEdgs() {
		return deltaEdgs;
	}

	public void setDeltaEdges(int[] deltaEdgs) {
		this.deltaEdgs = deltaEdgs;
	}

	public byte[] getDeltaVals() {
		return deltaVals;
	}

	public void setDeltaVals(byte[] deltaVals) {
		this.deltaVals = deltaVals;
	}

	// oldUnewUdelta

	private int oldUnewUdeltaEdgs[];
	private byte oldUnewUdeltaVals[];

	public int[] getOldUnewUdeltaEdgs() {
		return oldUnewUdeltaEdgs;
	}

	public void setOldUnewUdeltaEdgs(int[] oldUnewUdeltaEdgs) {
		this.oldUnewUdeltaEdgs = oldUnewUdeltaEdgs;
	}

	public byte[] getOldUnewUdeltaVals() {
		return oldUnewUdeltaVals;
	}

	public void setOldUnewUdeltaVals(byte[] oldUnewUdeltaVals) {
		this.oldUnewUdeltaVals = oldUnewUdeltaVals;
	}

	public ComputationSet(){
		this.oldEdgs = new int[0];
		this.oldVals = new byte[0];
		
		this.newEdgs = new int[0];
		this.newVals = new byte[0];
		
		this.oldUnewEdgs = new int[0];
		this.oldUnewVals = new byte[0];
		
		this.deltaEdgs = new int[0];
		this.deltaVals = new byte[0];
		
		this.oldUnewUdeltaEdgs = new int[0];
		this.oldUnewUdeltaVals = new byte[0];
	}
}
