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
	public void initScheduler(int numParts);

	/**
	 * 
	 * @return
	 */
	public int[] getPartstoLoad();

}
