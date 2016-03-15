package edu.uci.ics.cs.graspan.datastructures;

/**
 * 
 * @author Aftab 1 November 2015
 */
public class GlobalParams {

	// INPUT 1 : input graph full file name and path
	public static String baseFilename = "";

	// INPUT 2 : total number of partitions
	static int numParts;

	// INPUT 3 : number of partitions during each computation
	static int numPartsPerComputation;

	// INPUT 4 : The strategy for reloading partitions;
	// RELOAD_PLAN_1 - Reload all the requested partitions everytime,
	// regardless of which are already in the memory
	// RELOAD_PLAN_2 - Reload only the requested partitions that are not in
	// the memory. If a partition has been repartitioned, we consider it not to
	// be in the memory.
	// RELOAD_PLAN_3 - Reload only the requested partitions that are not in
	// the memory, however, if a partition has been repartitioned and a
	// requested partition is one of its child partitions, we keep the child
	// partition
	static String reloadPlan = "";

	private static final int GRAMMAR_SIZE = 200;

	// The grammar file
	public static byte[][] grammarTab = new byte[GRAMMAR_SIZE][3];

	// The size of the Edge Destination Count Table
	public static final int EDC_SIZE = 200;

	// Output edge tracker interval
	public static final int OUTPUT_EDGE_TRACKER_INTERVAL = 1000;

	// Size of each new edges node
	public static final int NEW_EDGE_NODE_SIZE = 100;

	// Maximum size of a partition after adding new edges
	public static final long PART_MAX_POST_NEW_EDGES = 400000;

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
	public static String getReloadPlan() {
		return reloadPlan;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setReloadPlan(String str) {
		reloadPlan = str;
	}

	/**
	 * 
	 * @return
	 */
	public static byte[][] getGrammarTab() {
		return grammarTab;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setGrammarTab(byte[][] arr) {
		grammarTab = arr;
	}

}
