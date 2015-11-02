package edu.uci.ics.cs.gdtc.userinput;

/**
 * 
 * @author Aftab 1 November 2015
 */
public class UserInput {

	// input graph full file name and path
	String baseFilename = "";

	// total number of partitions
	int numParts;

	// number of partitions during each computation
	int numPartsPerComputation;

	public void setBasefilename(String str) {
		baseFilename = str;
	}

	public String getBasefilename() {
		return baseFilename;
	}

	public void setNumParts(int n) {
		numParts = n;
	}

	public int getNumParts() {
		return numParts;
	}

	public void setNumPartsPerComputation(int n) {
		numPartsPerComputation = n;
	}

	public int getNumPartsPerComputation() {
		return numPartsPerComputation;
	}

}
