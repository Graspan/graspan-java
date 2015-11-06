package edu.uci.ics.cs.gdtc.userinput;

/**
 * 
 * @author Aftab 1 November 2015
 */
public class UserInput {

	// input graph full file name and path
	static String baseFilename = "";

	// total number of partitions
	static int numParts;

	// number of partitions during each computation
	static int numPartsPerComputation;

	// The strategy for reloading partitions:
	// RELOAD_STRATEGY1 - Reload all the requested partitions everytime,
	// regardless of which are already in the memory
	// RELOAD_STRATEGY2 - Reload only the requested partitions that are not in
	// the memory. If a partition has been repartitioned, we consider to be not
	// in the memory.
	// RELOAD_STRATEGY3 - Reload only the requested partitions that are not in
	// the memory, however, if a partition has been repartitioned and a
	// requested partition is one of its child partitions, we keep the child
	// partition
	static String partReloadStrategy = "";

	/**
	 * 
	 * @param str
	 */
	public static void setBasefilename(String str) {
		baseFilename = str;
	}

	/**
	 * 
	 * @return String baseFilename
	 */
	public static String getBasefilename() {
		return baseFilename;
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
	 * @return int numParts
	 */
	public static int getNumParts() {
		return numParts;
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
	 * @return int numPartsPerComputation
	 */
	public static int getNumPartsPerComputation() {
		return numPartsPerComputation;
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
	public static String getPartReloadStrategy() {
		return partReloadStrategy;
	}

}
