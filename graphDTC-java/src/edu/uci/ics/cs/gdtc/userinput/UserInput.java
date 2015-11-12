package edu.uci.ics.cs.gdtc.userinput;

/**
 * 
 * @author Aftab 1 November 2015
 */
public class UserInput {

	// INPUT 1 : input graph full file name and path
	static String baseFilename = "";

	// INPUT 2 : total number of partitions
	static int numParts;

	// INPUT 3 : number of partitions during each computation
	static int numPartsPerComputation;

	// INPUT 4 : The strategy for reloading partitions;
	// RELOAD_STRATEGY_1 - Reload all the requested partitions everytime,
	// regardless of which are already in the memory
	// RELOAD_STRATEGY_2 - Reload only the requested partitions that are not in
	// the memory. If a partition has been repartitioned, we consider it not to
	// be in the memory.
	// RELOAD_STRATEGY_3 - Reload only the requested partitions that are not in
	// the memory, however, if a partition has been repartitioned and a
	// requested partition is one of its child partitions, we keep the child
	// partition
	static String partReloadStrategy = "";

	// INPUT 5 : The strategy for preserving partitions;
	// PART_PRESERVE_STRATEGY_1 - User the same Vertices[] and edgelists arrays,
	// setting it initially by a preset size.
	static String loadedPartPreservationStrategy = "";

	/**
	 * 
	 * @return String baseFilename
	 */
	public static String getBasefilename() {
		return baseFilename;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setBasefilename(String str) {
		baseFilename = str;
	}

	/**
	 * 
	 * @return int numParts
	 */
	public static int getNumParts() {
		return numParts;
	}

	/**
	 * 
	 * @param n
	 */
	public static void setNumParts(int n) {
		numParts = n;
	}

	/**
	 * 
	 * @return int numPartsPerComputation
	 */
	public static int getNumPartsPerComputation() {
		return numPartsPerComputation;
	}

	/**
	 * 
	 * @param n
	 */
	public static void setNumPartsPerComputation(int n) {
		numPartsPerComputation = n;
	}

	/**
	 * 
	 * @return
	 */
	public static String getPartReloadStrategy() {
		return partReloadStrategy;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setPartReloadStrategy(String str) {
		partReloadStrategy = str;
	}

	/**
	 * 
	 * @return
	 */
	public static String getPartPreservationStrategy() {
		return loadedPartPreservationStrategy;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setPartPreservationStrategy(String str) {
		loadedPartPreservationStrategy = str;
	}

}
