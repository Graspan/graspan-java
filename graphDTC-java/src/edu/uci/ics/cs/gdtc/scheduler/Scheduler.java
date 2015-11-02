package edu.uci.ics.cs.gdtc.scheduler;

import java.util.ArrayList;

/**
 * @author Kai Wang
 *
 * Created by Oct 30, 2015
 */
public class Scheduler {
	private ArrayList<PartitionEdgeInfo> allEdgeInfo = new ArrayList<PartitionEdgeInfo>();
	private ArrayList<Long> partitionNumEdges = new ArrayList<Long>();
	
	/**
	 * Constructor
	 * @param partSizes
	 * @param edgeDstCount
	 * @exception 
	 */
	public Scheduler(long[] partSizes, long[][] edgeDstCount) {
		if(partSizes == null || edgeDstCount == null)
			throw new IllegalArgumentException("Null parameter in scheduler!");
		
		for(long size : partSizes)
			partitionNumEdges.add(Long.valueOf(size));
		
		for(int i = 0; i < edgeDstCount.length; i++) {
			PartitionEdgeInfo edgeInfo = new PartitionEdgeInfo(i, edgeDstCount[i]);
			allEdgeInfo.add(edgeInfo);
		}
		
	}
	
	private void computePriority() {
		assert(allEdgeInfo != null && partitionNumEdges != null);
		assert(allEdgeInfo.size() > 0 && partitionNumEdges.size() > 0);
		int size = allEdgeInfo.size();
		
		// each partition has an edgeInfo, iterate all partitions
		for(PartitionEdgeInfo edgeInfo : allEdgeInfo) {
			int partionId = edgeInfo.getPartitionId();
			// get edgeInfo and priorityInfo for each partition
			ArrayList<Long> pEdgeInfo = edgeInfo.getPartitionEdgeInfo();
			ArrayList<Long> priorityInfo = edgeInfo.getPriorityInfo();
			
			// compute priority, starting from (partitionId + 1)
			// for example, if there are 3 partitions, 0, 1, 2,
			// the current partition is 0, then consider(p0, p1), (p0, p2)
			for(int i = (partionId + 1); i < size; i++) {
				// get edgeInfo for the current partition
				long edgesCurrentPartition = pEdgeInfo.get(partionId);
				// get edgeInfo for the next partition
				long edgesNextPartition = pEdgeInfo.get(i);
				// compute priority and set priority
				priorityInfo.set(i, edgesCurrentPartition + edgesNextPartition);
			}
		}
		
	}
	
	public ArrayList<Integer> schedulePartitions() {
		computePriority();
		long maxPriority = 0L;
		int scheduledOne = -1;
		int scheduledTwo = -1;
		
		// schedule two partitions every time
		ArrayList<Integer> result = new ArrayList<Integer>(2);
		
		// each partition has an edgeInfo
		for(PartitionEdgeInfo edgeInfo : allEdgeInfo) {
			int partitionId = edgeInfo.getPartitionId();
			scheduledOne = partitionId;
			ArrayList<Long> priorityInfo = edgeInfo.getPriorityInfo();
			for(int i = 0; i < priorityInfo.size(); i++) {
				if(priorityInfo.get(i) > maxPriority) {
					maxPriority = priorityInfo.get(i);
					scheduledTwo = i;
				}
			}
		}
		
		if(scheduledOne == -1 && scheduledTwo == -1)
			return null;
		
		result.add(scheduledOne);
		result.add(scheduledTwo);
		return result;
	}
}
