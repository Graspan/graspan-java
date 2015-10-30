package edu.uci.ics.cs.gdtc.partitiondata;

public class AllPartitions {

	/*
	 * TODO Remember to update partAllocTable after repartitioning and reset all
	 * the other data structures after the end of a new edge computation
	 * iteration
	 */
	public static int[] partAllocTable;

	

	/**
	 * Initializes the partition allocation table
	 * 
	 * @param numParts
	 */
	public static void setPartAllocTab(int[] arr) {
		partAllocTable = arr;
	}

	/**
	 * Returns a ref to partAllocTable
	 * 
	 * @return
	 */
	public static int[] getPartAllocTab() {
		return partAllocTable;
	}

	
}