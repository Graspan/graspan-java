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

	public static void setBasefilename(String str) {
		baseFilename = str;
	}

	public static String getBasefilename() {
		return baseFilename;
	}

	public static void setNumParts(int n) {
		numParts = n;
	}

	public static int getNumParts() {
		return numParts;
	}

	public static void setNumPartsPerComputation(int n) {
		numPartsPerComputation = n;
	}

	public static int getNumPartsPerComputation() {
		return numPartsPerComputation;
	}

}
