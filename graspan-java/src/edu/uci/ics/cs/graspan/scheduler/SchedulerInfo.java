package edu.uci.ics.cs.graspan.scheduler;

public class SchedulerInfo {

	public static long[] partSizes;
	private static long[][] edgeDestCount;
	private int[][] terminationMap;

	/**
	 * 
	 * @param arr
	 */
	public static void setPartSizes(long[] arr) {
		partSizes = arr;
	}

	/**
	 * 
	 * @return long[] partSizes
	 */
	public static long[] getPartSizes() {
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
}
