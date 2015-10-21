package edu.uci.ics.cs.gdtc.enginedata;

public class LoadedPartitions {

	// Data structures for storing the partitions to load:
	// Dimension 1 indicates partition number, Dimension 2 indicates list for a
	// source vertex, Dimension 3 indicates an out-edge from each source vertex
	private static int partEdgeArrays[][][];
	private static byte partEdgeValArrays[][][];

	// Stores the out degrees of each source vertex of each partition.
	// Dimension 1 indicates partition number, Dimension 2 indicates out degree
	// of a source vertex in the partition indicated by Index 1
	private static int partOutDegrees[][];

	// Contains the ids of the partitions to be loaded in the memory
	private static int loadedParts[];

	/**
	 * 
	 * @return
	 */
	public static int[][][] getPartEdgeArrays() {
		return partEdgeArrays;
	}

	/**
	 * 
	 * @param arr3d
	 */
	public static void setPartEdgeValArrays(byte[][][] arr3d) {
		partEdgeValArrays = arr3d;
	}

	/**
	 * 
	 * @return
	 */
	public static byte[][][] getPartEdgeValArrays() {
		return partEdgeValArrays;
	}

	/**
	 * 
	 * @param arr3d
	 */
	public static void setPartEdgeArrays(int[][][] arr3d) {
		partEdgeArrays = arr3d;
	}

	/**
	 * 
	 * @return
	 */
	public static int[][] getPartOutDegs() {
		return partOutDegrees;
	}

	/**
	 * 
	 * @param arr2d
	 */
	public static void setPartOutDegs(int[][] arr2d) {
		partOutDegrees = arr2d;
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
