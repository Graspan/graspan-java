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

}
