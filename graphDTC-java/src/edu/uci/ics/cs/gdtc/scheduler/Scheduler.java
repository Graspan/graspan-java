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
}
