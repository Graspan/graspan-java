package edu.uci.ics.cs.graspan.scheduler;

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
	
	public PartitionEdgeInfo(int partitionId, int size) {
		this.partitionId = partitionId;
		for(int i = 0; i < size; i++)
			terminationInfo.add(Boolean.FALSE);
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
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");
		result.append(NEW_LINE + "partition id : " + partitionId);
		result.append(NEW_LINE + "termination info : " + terminationInfo);
		
		return result.toString();
		
	}
}
