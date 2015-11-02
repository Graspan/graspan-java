package edu.uci.ics.cs.gdtc.scheduler;

import java.util.ArrayList;

/**
 * @author Kai Wang
 *
 * Created by Oct 30, 2015
 */
public class PartitionEdgeInfo {
	private int partitionId;
	private ArrayList<Long> pEdgeInfo = new ArrayList<Long>();
	private ArrayList<Long> priorityInfo = new ArrayList<Long>();
	
	public PartitionEdgeInfo(int partitionId) {
		this.partitionId = partitionId;
	}
	
	public PartitionEdgeInfo(int partitionId, long[] edgeInfo) {
		if(edgeInfo == null)
			throw new IllegalArgumentException("Null parameter in PartitionEdgeInfo!");
		
		this.partitionId = partitionId;
		for(long info : edgeInfo) {
			pEdgeInfo.add(Long.valueOf(info));
			priorityInfo.add(0L);
		}
	}
	
	public ArrayList<Long> getPartitionEdgeInfo() {
		return pEdgeInfo;
	}
	
	public int getPartitionId() {
		return partitionId;
	}
	
	public ArrayList<Long> getPriorityInfo() {
		return priorityInfo;
	}
}
