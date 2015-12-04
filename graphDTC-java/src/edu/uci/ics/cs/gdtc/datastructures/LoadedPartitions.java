package edu.uci.ics.cs.gdtc.datastructures;

import java.util.HashSet;
import java.util.LinkedHashSet;

/**
 * This class maintains all the variables used for loaded partitions.
 * 
 * @author Aftab
 *
 */
public class LoadedPartitions {

	// Contains the ids of the partitions loaded in the memory
	private static int loadedParts[];

	// Partitions to save to disk
	private static HashSet<Integer> savePartsSet;

	// Loaded partitions that have been repartitioned
	private static HashSet<Integer> repartitionedParts;

	// Contains the ids of the partitions that are to be loaded in the memory
	private static int newParts[];

	// Stores the out degrees of each source vertex of each partition.
	// Dimension 1 indicates partition number, Dimension 2 indicates out degree
	// of a source vertex in the partition indicated by Index 1
	private static int loadedPartOutDegs[][];

	// Data structures for storing the partitions to load:
	// Dimension 1 indicates partition number, Dimension 2 indicates list for a
	// source vertex, Dimension 3 indicates an out-edge from each source vertex
	private static int loadedPartEdges[][][];
	private static byte loadedPartEdgeVals[][][];

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
	 * @return byte[][][] loadedPartEdgeVals
	 */
	public static byte[][][] getLoadedPartEdgeVals() {
		return loadedPartEdgeVals;
	}

	/**
	 * Saves the loaded partitions in 3D arrays.
	 * 
	 * @param arr3d
	 */
	public static void setLoadedPartEdges(int[][][] arr3d) {
		loadedPartEdges = arr3d;
	}

	/**
	 * 
	 * @return int[][] loadedPartOutDegs
	 */
	public static int[][] getLoadedPartOutDegs() {
		return loadedPartOutDegs;
	}

	/**
	 * 
	 */
	public static void printLoadedPartOutDegs() {
		System.out.println("*****");
		for (int i = 0; i < loadedParts.length; i++) {
			System.out.println("Source vertex degrees for partition " + loadedParts[i]);
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(loadedParts[i]); j++) {
				System.out.println(loadedPartOutDegs[i][j]);
			}
			System.out.println();
		}
		System.out.println("*****");
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
	 * @return int[] loadedParts
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

	/**
	 * 
	 * @return int[] newParts
	 */
	public static int[] getNewParts() {
		return newParts;
	}

	/**
	 * 
	 * @param arr
	 */
	public static void setNewParts(int[] arr) {
		newParts = arr;
	}

	/**
	 * Prints the set of partitions to load
	 */
	public static void printNewParts() {
		System.out.println("*****");
		System.out.println("New partitions to load:");
		for (int i = 0; i < newParts.length; i++) {
			System.out.println(newParts[i]);
		}
		System.out.println("*****");
	}

	/**
	 * Print the loaded partitions
	 */
	public static void printLoadedPartitions() {
		System.out.println("*****");
		System.out.println("Loaded partitions:");
		for (int i = 0; i < loadedParts.length; i++) {
			System.out.println("--- Partition: " + loadedParts[i] + " ---");
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(loadedParts[i]); j++) {
				int srcv = j + PartitionQuerier.getFirstSrc(loadedParts[i]);
				System.out.println("SourceV: " + srcv);
				System.out.println("Dest Vs: ");
				for (int k = 0; k < loadedPartOutDegs[i][j]; k++) {
					System.out.print(loadedPartEdges[i][j][k] + " ");
				}
				System.out.println();
				System.out.println("Edge Vals: ");
				for (int k = 0; k < loadedPartOutDegs[i][j]; k++) {
					System.out.print(loadedPartEdgeVals[i][j][k] + " ");
				}
				System.out.println();
			}
			System.out.println();
		}
		System.out.println("*****");
	}

	public static void setPartsToSave(HashSet<Integer> set) {
		savePartsSet = set;

	}

	public static HashSet<Integer> getPartsToSave() {
		return savePartsSet;
	}

	public static void setRepartitionedParts(HashSet<Integer> set) {
		repartitionedParts = set;

	}

	public static HashSet<Integer> getRepartitionedParts() {
		return repartitionedParts;
	}
	
	

}