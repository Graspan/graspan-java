package edu.uci.ics.cs.gdtc.scheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Kai Wang
 *
 * Created by Oct 30, 2015
 */
public class PartitionEdgeInfo {
	private int partitionId;
	private List<Long> pEdgeInfo = new ArrayList<Long>();
	private List<Long> priorityInfo = new ArrayList<Long>();
	private List<Boolean> terminationInfo = new ArrayList<Boolean>();
	
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
			terminationInfo.add(Boolean.FALSE);
		}
	}
	
	public List<Long> getPartitionEdgeInfo() {
		return pEdgeInfo;
	}
	
	public int getPartitionId() {
		return partitionId;
	}
	
	public List<Long> getPriorityInfo() {
		return priorityInfo;
	}
	
	public List<Boolean> getTerminationInfo() {
		return terminationInfo;
	}
}