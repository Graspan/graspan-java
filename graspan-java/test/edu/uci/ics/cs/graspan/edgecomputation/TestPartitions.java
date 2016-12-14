package edu.uci.ics.cs.graspan.edgecomputation;

/**
 * This program creates the data structures for 3 artificial partitions.
 * 
 * @author Aftab
 *
 */
public class TestPartitions {

	public int partAllocTable[] = { 7, 8, 15, 20, 32, 43 };

	// Contains the ids of the partitions to be loaded in the memory
	public int loadedParts[] = { 0, 2, 5 };

	// Data structures for storing the partitions to load:
	// Dimension 1 indicates partition number, Dimension 2 indicates list for a
	// source vertex, Dimension 3 indicates an out-edge from each source vertex
	public int partEdgeArrays[][][] = new int[3][][];
	public int partEdgeValArrays[][][] = new int[3][][];

	// Stores the out degrees of each source vertex of each partition.
	// Dimension 1 indicates partition number, Dimension 2 indicates out degree
	// of a source vertex in the partition indicated by Index 1
	public int partOutDegrees[][] = new int[3][];

	public void createPartitions() {

		// create outDegrees
		partOutDegrees[0] = new int[] { 6, 5, 0, 6, 4, 4, 5 };// 7 srcs
		partOutDegrees[1] = new int[] { 6, 9, 0, 15, 0, 2, 10 };// 7 srcs
		partOutDegrees[2] = new int[] { 3, 3, 2, 10, 12, 9, 8, 0, 7, 7, 0 };// 11
																			// srcs

		// create Partition 0 Edges
		partEdgeArrays[0][0] = new int[] { 3, 5, 6, 10, 12 };
		partEdgeArrays[0][1] = new int[] { 2, 7, 80, 10, 33, 33 };
		partEdgeArrays[0][2] = new int[] {};
		partEdgeArrays[0][3] = new int[] { 12, 12, 112, 12, 12, 12 };
		partEdgeArrays[0][4] = new int[] { 1, 2, 4, 5 };
		partEdgeArrays[0][5] = new int[] { 38, 39, 42, 43 };
		partEdgeArrays[0][6] = new int[] { 8, 18, 12, 15, 152 };

		// create Partition 0 Edge Values
		partEdgeValArrays[0][0] = new int[] { 7, 100, 101, 1, 5 };
		partEdgeValArrays[0][1] = new int[] { 9, 11, 1, 10, 2, 8 };
		partEdgeValArrays[0][2] = new int[] {};
		partEdgeValArrays[0][3] = new int[] { 1, 2, 3, 4, 5, 6 };
		partEdgeValArrays[0][4] = new int[] { 7, 7, 7, 7 };
		partEdgeValArrays[0][5] = new int[] { 1, 1, 5, 9 };
		partEdgeValArrays[0][6] = new int[] { 1, 2, 5, 3, 2 };

		// create Partition 2 Edges
		partEdgeArrays[1][0] = new int[] { 1, 2, 3, 9, 10, 33 };
		partEdgeArrays[1][1] = new int[] { 1, 2, 3, 5, 17, 19, 110, 33, 34 };
		partEdgeArrays[1][2] = new int[] {};
		partEdgeArrays[1][3] = new int[] { 1, 1, 2, 9, 10, 10, 33, 34, 42, 42, 42, 42, 43, 43, 43 };
		partEdgeArrays[1][4] = new int[] {};
		partEdgeArrays[1][5] = new int[] { 37, 39 };
		partEdgeArrays[1][6] = new int[] { 1, 2, 3, 4, 5, 5, 5, 5, 6, 7 };

		// create Partition 2 Edge Values
		partEdgeValArrays[1][0] = new int[] { 1, 1, 2, 5, 7, 3 };
		partEdgeValArrays[1][1] = new int[] { 1, 1, 2, 7, 11, 11, 12, 2, 3 };
		partEdgeValArrays[1][2] = new int[] {};
		partEdgeValArrays[1][3] = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
		partEdgeValArrays[1][4] = new int[] {};
		partEdgeValArrays[1][5] = new int[] { 14, 1 };
		partEdgeValArrays[1][6] = new int[] { 5, 5, 5, 5, 5, 6, 7, 8, 9, 9 };

		// create Partition 5 Edges
		partEdgeArrays[2][0] = new int[] { 12, 40, 41 };
		partEdgeArrays[2][1] = new int[] { 4, 10, 12 };
		partEdgeArrays[2][2] = new int[] { 12, 15 };
		partEdgeArrays[2][3] = new int[] { 1, 2, 4, 7, 10, 11, 12, 15, 36, 41 };
		partEdgeArrays[2][4] = new int[] { 1, 2, 3, 10, 13, 14, 15, 33, 36, 36, 37, 38 };
		partEdgeArrays[2][5] = new int[] { 11, 12, 13, 36, 37, 38, 39, 40, 42 };
		partEdgeArrays[2][6] = new int[] { 6, 7, 9, 10, 12, 15, 37, 38 };
		partEdgeArrays[2][7] = new int[] {};
		partEdgeArrays[2][8] = new int[] { 4, 5, 6, 7, 7, 10, 11 };
		partEdgeArrays[2][9] = new int[] { 7, 15, 39, 40, 41, 42, 42 };
		partEdgeArrays[2][10] = new int[] {};

		// create Partition 5 Edge Values
		partEdgeValArrays[2][0] = new int[] { 5, 6, 7 };
		partEdgeValArrays[2][1] = new int[] { 100, 100, 50 };
		partEdgeValArrays[2][2] = new int[] { 10, 6 };
		partEdgeValArrays[2][3] = new int[] { 5, 5, 5, 3, 2, 10, 11, 8, 1, 2 };
		partEdgeValArrays[2][4] = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 10 };
		partEdgeValArrays[2][5] = new int[] { 10, 11, 12, 13, 14, 15, 16, 17, 18 };
		partEdgeValArrays[2][6] = new int[] { 8, 8, 8, 8, 1, 2, 3, 4 };
		partEdgeValArrays[2][7] = new int[] {};
		partEdgeValArrays[2][8] = new int[] { 1, 2, 3, 13, 2, 6, 10 };
		partEdgeValArrays[2][9] = new int[] { 10, 20, 30, 3, 5, 6, 7 };
		partEdgeValArrays[2][10] = new int[] {};
	}
}
