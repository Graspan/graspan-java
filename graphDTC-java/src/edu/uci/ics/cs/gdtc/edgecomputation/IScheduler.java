package edu.uci.ics.cs.gdtc.edgecomputation;

/**
 * 
 * @author Aftab
 *
 */
public interface IScheduler {
	
	/**
	 * Initializes the Scheduler with the initial number of input partitions.
	 * 
	 * @param numParts
	 */
	public void initScheduler(int totalNumParts);

	/**
	 * 
	 * @return
	 */
	public int[] getPartstoLoad(int numPartsPerComputation);

}
