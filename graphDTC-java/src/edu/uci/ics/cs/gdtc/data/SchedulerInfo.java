package edu.uci.ics.cs.gdtc.data;

public class SchedulerInfo {

	public static long[] partSizes;
	private static long[][] edgeDestCount;

	/**
	 * 
	 * @param arr
	 */
	public static void setPartSizes(long[] arr) {
		partSizes = arr;
	}

	/**
	 * Returns a ref to partSizes
	 * 
	 * @return
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
	 * Returns a ref to edgeDestCount
	 * 
	 * @param arr
	 */
	public static long[][] getEdgeDestCount() {
		return edgeDestCount;
	}
}
