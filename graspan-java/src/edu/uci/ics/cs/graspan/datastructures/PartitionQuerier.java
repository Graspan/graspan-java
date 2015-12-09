package edu.uci.ics.cs.graspan.datastructures;

import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * Contains of methods for retrieving miscellaneous info of loaded partitions
 * (used by all phases)
 * 
 * @author Aftab
 */

// TODO NEED TO UPDATE THE LOGIC OF ALL FUNCTIONS HERE, IF PARTITION ALLOCATION
// TABLE IS NOT CONTIGUOUS BY PARTID// u cannot operate on partition id anymore
public class PartitionQuerier {

	private static final Logger logger = GraspanLogger.getLogger("partitionquerier");

	/**
	 * Returns the number of unique sources of in partition partId. IMP: we
	 * consider the vertex numbering of the input graph to start from 1 NOT 0.
	 * 
	 * @param partId
	 */
	public static int getNumUniqueSrcs(int partId) {
		int[][] partAllocTable = AllPartitions.getPartAllocTab();
		for (int i = 0; i < partAllocTable.length; i++) {
			if (partId == partAllocTable[i][0]) {
				if (i == 0) {
					return partAllocTable[i][1];
				} else {
					return partAllocTable[i][1] - partAllocTable[i - 1][1];
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the smallest source vertex Id in partition partId.
	 * 
	 * @param partId
	 * @return
	 */
	public static int getFirstSrc(int partId) {
		int[][] partAllocTable = AllPartitions.getPartAllocTab();
		for (int i = 0; i < partAllocTable.length; i++) {
			if (partId == partAllocTable[i][0]) {
				if (i == 0) {
					return 1;
				} else {
					return partAllocTable[i - 1][1] + 1;
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the largest source vertex Id in partition partId.
	 * 
	 * @param partId
	 * @return
	 */
	public static int getLastSrc(int partId) {
		int[][] partAllocTable = AllPartitions.getPartAllocTab();
		for (int i = 0; i < partAllocTable.length; i++) {
			if (partId == partAllocTable[i][0]) {
				return partAllocTable[i][1];
			}
		}
		logger.info("ERROR: Last source is -1 for partition " + partId);
		return -1;
	}

	/**
	 * Finds whether the given vertex belongs to a partition as a source vertex.
	 * 
	 * @param srcVId
	 * @param partId
	 * @return
	 */
	public static boolean inPartition(int srcVId, int partId) {
		if (srcVId >= getFirstSrc(partId) & srcVId <= getLastSrc(partId))
			return true;
		else
			return false;
	}

	/**
	 * 
	 * Returns the actual Id of the source vertex from the Id of the vertex in
	 * the loaded partition array.
	 * 
	 * @param vertexPartArrIdx
	 * @param partId
	 * @return
	 */
	public static int getActualIdFrmPartArrIdx(int vertexPartArrIdx, int partId) {
		if (findPartition(vertexPartArrIdx + getFirstSrc(partId)) != partId) {
			logger.info("ERROR: The " + vertexPartArrIdx + "th element of partition " + partId + "does not exist");
			return -1;
		}
		return vertexPartArrIdx + getFirstSrc(partId);
	}

	/**
	 * Returns the Id of the source vertex in the loaded partition array from
	 * the actual Id of the source vertex.
	 * 
	 * @param src
	 *            - the actual Id of the source vertex.
	 * @param partId
	 *            - the partition Id.
	 * @return
	 */
	public static int getPartArrIdxFrmActualId(int src, int partId) {
		if (findPartition(src) != partId) {
			logger.info("ERROR: Source " + src + " does not exist in partition " + partId);
			return -1;
		}
		return src - getFirstSrc(partId);
	}

	/**
	 * Returns the partition id of a given source vertex. Returns -1 if vertex
	 * does not exist in any partition as a source vertex.
	 * 
	 * @param src
	 *            - the actual Id of the source vertex.
	 */
	public static int findPartition(int src) {
		int[][] partAllocTable = AllPartitions.getPartAllocTab();
		for (int i = 0; i < partAllocTable.length; i++) {
			if (src <= partAllocTable[i][1]) {
				return partAllocTable[i][0];
			}
		}
		return -1;
	}

}