package edu.uci.ics.cs.gdtc.engine;

import edu.uci.ics.cs.gdtc.enginedata.AllPartitions;

/**
 * Contains of methods for retrieving miscellaneous info of loaded partitions
 * (used by all phases)
 * 
 * @author Aftab
 */
public class PartitionQuerier extends PartitionLoader {

	/**
	 * Returns the number of unique sources of in partition partId. IMP: we
	 * consider the vertex numbering of the input graph to start from 1 NOT 0
	 * 
	 * @param partId
	 */
	public static int getNumUniqueSrcs(int partId) {
		if (partId == 0) {
			return getMaxSrc(partId);
		} else {
			return getMaxSrc(partId) - getMaxSrc(partId - 1);
		}
	}

	/**
	 * Returns the smallest source vertex Id in partition partId
	 * 
	 * @param partId
	 * @return
	 */
	public static int getMinSrc(int partId) {
		if (partId == 0) {
			return 1;
		} else {
			return (getMaxSrc(partId - 1) + 1);
		}
	}

	/**
	 * Returns the largest source vertex Id in partition partId
	 * 
	 * @param partId
	 * @return
	 */
	public static int getMaxSrc(int partId) {
		int[] partAllocTable = AllPartitions.getPartAllocTab();
		int maxSrc = partAllocTable[partId];
		return maxSrc;
	}

	/**
	 * Finds whether the given vertex belongs to a partition
	 * 
	 * @param srcVId
	 * @param partId
	 * @return
	 */
	public static boolean inPartition(int srcVId, int partId) {
		if (srcVId >= getMinSrc(partId) & srcVId <= getMaxSrc(partId))
			return true;
		else
			return false;
	}

	/**
	 * 
	 * Returns the actual Id of the source vertex from the Id of the vertex in
	 * the loaded partition array
	 * 
	 * @param vertexPartArrId
	 * @param partId
	 * @return
	 */
	public static int getActualIdFrmPartArrId(int vertexPartArrId, int partId) {
		return vertexPartArrId + getMinSrc(partId);
	}

	/**
	 * Returns the Id of the source vertex in the loaded partition array from
	 * the actual Id of the source vertex
	 * 
	 * @param vertexId
	 * @param partId
	 * @return
	 */
	public static int getPartArrIdFrmActualId(int vertexId, int partId) {
		return getMaxSrc(partId) - vertexId;
	}

	

	/**
	 * searches the partAllocTable to find out which partition a vertex belongs
	 * (CALLED BY addEdgetoBuffer() method)
	 * 
	 * @param srcV
	 */
	public static int findPartition(int srcVId) {
		int[] partAllocTable = AllPartitions.getPartAllocTab();
		int partitionId = -1;
		for (int i = 0; i < partAllocTable.length; i++) {
			if (srcVId <= partAllocTable[i]) {
				partitionId = i;
				break;
			}
		}
		return partitionId;
	}

}
