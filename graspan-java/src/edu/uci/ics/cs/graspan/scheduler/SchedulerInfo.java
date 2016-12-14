package edu.uci.ics.cs.graspan.scheduler;

import java.util.LinkedHashMap;

public class SchedulerInfo {

	private static long[][] partSizes;
	private static long[][] edgeDestCount;
	private static long[][] edgeDestCountTwoWay;
	private int[][] terminationMap;
	private static double[][] edcPercentage = new double[50][50];

	public static LinkedHashMap<Integer, LinkedHashMap<Integer, Long>> edgeDestCountMP = 
			new LinkedHashMap<Integer, LinkedHashMap<Integer, Long>>();
	
	/**
	 * 
	 * @param arr
	 */
	public static void setPartSizes(long[][] arr) {
		partSizes = arr;
	}

	/**
	 * 
	 * @return long[] partSizes
	 */
	public static long[][] getPartSizes() {
		return partSizes;
	}

	/**
	 * 
	 * @param arr
	 */
	public static void setEdgeDestCount(long[][] arr) {
		edgeDestCount = arr;
	}

	/**
	 * 
	 * @return long[][] edgeDestCount
	 */
	public static long[][] getEdgeDestCount() {
		return edgeDestCount;
	}

	/**
	 * 
	 * @return
	 */
	public static double[][] getEdcPercentage() {
		return edcPercentage;
	}

	/**
	 * 
	 * @param arr
	 */
	public static void setEdcPercentage(double[][] arr) {
		edcPercentage = arr;
	}

	/**
	 * Prints the partition sizes and the partition edge counts
	 */
	public static void printData() {
		System.out.println("Printing Preliminary Scheduling Info...");
		System.out.println("Partition sizes: ");
		for (int i = 0; i < partSizes.length; i++) {
			System.out.println(partSizes[i]);
		}

		System.out.println("Partition edge destination counts: ");
		for (int i = 0; i < partSizes.length; i++) {
			for (int j = 0; j < partSizes.length; j++) {
				System.out.println(i + " " + j + " " + edgeDestCount[i][j]);
			}
		}
	}

	public static void setEdcTwoWay(long[][] edcTwoWay) {
		edgeDestCountTwoWay=edcTwoWay;
	}
	
	public static long[][] getEdcTwoWay(){
		return edgeDestCountTwoWay;
	}

	// @Override
	// public String toString() {
	// StringBuilder result = new StringBuilder();
	// String NEW_LINE = System.getProperty("line.separator");
	//
	// for (int i = 0; i < 50; i++) {
	// result.append(NEW_LINE + i + " : ");
	// for (int j = 0; j < 50; j++) {
	// result.append("(" + j + "," + edgeDestCount[i][j] + ") ");
	// }
	// }
	// return result.toString();
	// }
}
