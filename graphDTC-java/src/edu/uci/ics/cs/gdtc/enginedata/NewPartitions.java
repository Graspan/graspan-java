package edu.uci.ics.cs.gdtc.enginedata;

public class NewPartitions {
	
	/*
	 * New Edge Data Structures
	 */

	// stores the new edges computed
	private int newEdgeArraySet[][];
	private static final int NUM_NEW_EDGE_ARRAY_SETS = 50;
	private static final int SIZEOF_NEW_EDGE_ARRAY_SET = 10;

	// indicates whether a new edge array set has been accessed by a thread
	private int newEdgeArrSetStatus[];

	/*
	 * Stores the indices of the new edges added for each src vertex. Dimension
	 * 1 - partition id | Dimension 2 - src row id in the partition | Dimension
	 * 3 - id of the first newEdgeArraySet that consists of new edges of this
	 * src (as per dim. 2) | Dimension 4 & 5, position of last new edge in
	 * newEdgeArraySet for this src.
	 */
	private int newEdgeArrMarkersforSrc[][][][][];
}
