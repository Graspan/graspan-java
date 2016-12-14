package edu.uci.ics.cs.graspan.scheduler;

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
	public int[] getPartstoLoad();

	/**
	 * Update Scheduler map
	 */
	public void updateSchedulerInfo();

}
