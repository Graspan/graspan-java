package edu.uci.ics.cs.gdtc.engine.data;

public class LoadedPartitions {

	// Data structures for storing the partitions to load:
	// Dimension 1 indicates partition number, Dimension 2 indicates list for a
	// source vertex, Dimension 3 indicates an out-edge from each source vertex
	private static int loadedPartEdges[][][];
	private static byte loadedPartEdgeVals[][][];

	// Stores the out degrees of each source vertex of each partition.
	// Dimension 1 indicates partition number, Dimension 2 indicates out degree
	// of a source vertex in the partition indicated by Index 1
	private static int loadedPartOutDegs[][];

	// Contains the ids of the partitions to be loaded in the memory
	private static int loadedParts[];

	/**
	 * 
	 * @return
	 */
	public static int[][][] getLoadedPartEdges() {
		return loadedPartEdges;
	}

	/**
	 * 
	 * @param arr3d
	 */
	public static void setLoadedPartEdgeVals(byte[][][] arr3d) {
		loadedPartEdgeVals = arr3d;
	}

	/**
	 * 
	 * @return
	 */
	public static byte[][][] getLoadedPartEdgeVals() {
		return loadedPartEdgeVals;
	}

	/**
	 * 
	 * @param arr3d
	 */
	public static void setLoadedPartEdges(int[][][] arr3d) {
		loadedPartEdges = arr3d;
	}

	/**
	 * 
	 * @return
	 */
	public static int[][] getLoadedPartOutDegs() {
		return loadedPartOutDegs;
	}

	/**
	 * 
	 * @param arr2d
	 */
	public static void setLoadedPartOutDegs(int[][] arr2d) {
		loadedPartOutDegs = arr2d;
	}

	/**
	 * 
	 * @return
	 */
	public static int[] getLoadedParts() {
		return loadedParts;
	}

	/**
	 * 
	 * @param arr
	 */
	public static void setLoadedParts(int[] arr) {
		loadedParts = arr;
	}

}
