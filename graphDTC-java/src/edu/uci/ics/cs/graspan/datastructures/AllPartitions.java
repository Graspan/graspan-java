package edu.uci.ics.cs.graspan.datastructures;

public class AllPartitions {

	/*
	 * TODO Remember to update partAllocTable after repartitioning and reset all
	 * the other data structures after the end of a new edge computation
	 * iteration
	 */
	public static int[][] partAllocTable;

	/**
	 * Sets the partition allocation table.
	 * 
	 * @param arr
	 */
	public static void setPartAllocTab(int[][] arr) {
		partAllocTable = arr;
	}

	/**
	 * Returns a ref to partAllocTable.
	 * 
	 * @return int[] partAllocTable
	 */
	public static int[][] getPartAllocTab() {
		return partAllocTable;
	}

	/**
	 * Prints the partition allocation table.
	 */
	public static void printPartAllocTab() {
		System.out.println("Partition allocation table:");
		for (int i = 0; i < partAllocTable.length; i++) {
			System.out.println(partAllocTable[i]);
		}
	}

}