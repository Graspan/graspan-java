package edu.uci.ics.cs.gdtc.edgecomputation;

/**
 * 
 * @author Aftab
 *
 */
public class BasicScheduler implements IScheduler {

	private int[][] partScheduleMap = new int[SizeOfPartScheduleMap][SizeOfPartScheduleMap];
	private static final int SizeOfPartScheduleMap = 50;

	private int[] newPartsIndex;
	private static final int NumOfNewParts = 10;

	private int numPartitions;

	/**
	 * Initializes the scheduler
	 * 
	 * entry 0:no active partition is represented by this row/column|entry
	 * -1:partition pair has not been computed|entry 1:partition pair has been
	 * computed
	 */
	public void initScheduler(int numPartitions) {
		// initialize partScheduleMap
		for (int i = 0; i < numPartitions; i++) {
			for (int j = 0; j < numPartitions; j++) {
				partScheduleMap[i][j] = -1;
			}
		}
		this.numPartitions = numPartitions;
	}

	public void updateSchedulerInfo() {
		
		

	}

	/**
	 * Returns the next pair of partitions (ids) to be computed
	 * 
	 */
	public int[] getPartstoLoad() {
		for (int i = 0; i < partScheduleMap.length; i++) {
			for (int j = 0; j < partScheduleMap.length; j++) {
				if (i != j & !isComputed(i, j)) {
					int[] partPair = new int[2];
					partPair[0] = i;
					partPair[1] = j;
					return partPair;
				}
			}
		}
		return null;
	}

	/**
	 * Checks whether the partition-pair has been computed. Called by
	 * getPartstoLoad()
	 * 
	 * @param part1
	 * @param part2
	 * @return
	 */
	private boolean isComputed(int part1mapId, int part2mapId) {
		if (partScheduleMap[part1mapId][part2mapId] == 1)
			return true;
		else
			return false;
	}

	/**
	 * Finds the corresponding actual PartitionId from ParitionMapId
	 * (Incomplete)
	 * 
	 * @param partMapId
	 * @return
	 */
	@SuppressWarnings("unused")
	private int findPartitionId(int partMapId) {
		if (!isOriginalPartition(partMapId)) {
			// TODO
			return 0;
		} else
			return partMapId;

	}

	/**
	 * Checks whether the partition belongs to the original set of input
	 * partitions
	 * 
	 * @param partMapId
	 * @return
	 */
	private boolean isOriginalPartition(int partMapId) {
		if (partMapId < this.numPartitions)
			return true;
		else
			return false;
	}
}
