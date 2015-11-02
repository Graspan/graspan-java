package edu.uci.ics.cs.gdtc.scheduler;

/**
 * 
 * @author Aftab
 *
 */
public interface IScheduler {

	/**
	 * Initializes the Scheduler with the initial number of input partitions.
	 * 
	 */
	public void initScheduler();

	/**
	 * 
	 * @return
	 */
	public int[] getPartstoLoad(int numPartsPerComputation);

}
