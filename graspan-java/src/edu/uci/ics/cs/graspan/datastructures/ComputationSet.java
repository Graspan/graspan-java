package edu.uci.ics.cs.graspan.datastructures;

/**
 * 
 * @author aftab
 * 
 *         Created Mar 20, 2016
 * 
 */
public class ComputationSet {

	// old

	private int oldEdgs[]=new int[0];
	private byte oldVals[]=new byte[0];

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

	private int newEdgs[]=new int[0];
	private byte newVals[]=new byte[0];

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

	// delta

	private int deltaEdgs[]=new int[0];
	private byte deltaVals[]=new byte[0];

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

	// oldUnew

	private int oldUnewEdgs[]=new int[0];
	private byte oldUnewVals[]=new byte[0];

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

	// oldUnewUdelta

	private int oldUnewUdeltaEdgs[]=new int[0];
	private byte oldUnewUdeltaVals[]=new byte[0];

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

}
