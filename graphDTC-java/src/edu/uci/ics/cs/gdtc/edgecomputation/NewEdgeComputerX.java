package edu.uci.ics.cs.gdtc.edgecomputation;

public class NewEdgeComputerX {

	private static final int NEW_EDGE_BUFFER_SIZE = 10;
	private static final int NEW_EDGE_INDEX_BUFFER_SIZE = 10;

	public int partEdgeArrays[][][]=;
	public int partEdgeValArrays[][][];
	public int partOutDegrees[][];

	/*
	 * New Edge Data Structures
	 */
	int partNewEdges[] = new int[NEW_EDGE_BUFFER_SIZE];
	int partNewEdgeVals[] = new int[NEW_EDGE_BUFFER_SIZE];
	int partNewEdgeIndex[] = new int[NEW_EDGE_INDEX_BUFFER_SIZE];
	int lastAddedEdgePos;

	/**
	 * Scans the edges of partition 1, checks for possible new edges, and adds
	 * them to the new edge arrays
	 */
	public void computeNewEdges( int[][][] partEdgeArrays, int[][][] partEdgeValArrays, int[][] partOutDegrees) {
		
		
		
		
		for (int i = 0; i < this.part1EdgesArray.length; i++) {
			for (int j = 0; j < this.part1degs[i]; j++) {

				// Edge 1
				int srcV1 = i + this.part1MinSrc;
				int destV1 = this.part1EdgesArray[i][j];
				int edgeVal1 = this.part1EdgeValsArray[i][j];

				// Edge 2 Source V
				int srcV2 = destV1;

				// if srcV2 is not a source vertex of either of the loaded
				// partitions,
				// ignore the computation for Edge 1
				if (!isInPartition(srcV2, part1MinSrc, part1MaxSrc) & !isInPartition(srcV2, part2MinSrc, part2MaxSrc)) {
					continue;
				}
				// else check for possible new edges
				else {

					// if srcV2 belongs to part1
					if (isInPartition(srcV2, part1MinSrc, part1MaxSrc)) {
						int srcV2IndxInPart1 = srcV2 - part1MinSrc;

						// for each destination from srcV2 as given by the
						// corresponding list from part 1
						for (int k = 0; k < this.part1degs[srcV2IndxInPart1]; k++) {
							int destV2 = part1EdgesArray[srcV2IndxInPart1][k];
							int edgeVal2 = part1EdgeValsArray[srcV2IndxInPart1][k];
							if (!isValidNewEdge(srcV1, destV2, edgeVal1, edgeVal2)) {
								break;
							} else {
								if (!newEdgeExists) {

								}
							}
							;
						}
					}
					// if srcV2 belongs to part2
					else {
						int srcV2IndxInPart2 = srcV2 - part2MinSrc;

						// for each destination from srcV2 as given by the
						// corresponding list from part 1
						for (int k = 0; k < this.part2degs[srcV2IndxInPart2]; k++) {
							int destV2 = part2EdgesArray[srcV2IndxInPart2][k];
							int edgeVal2 = part2EdgeValsArray[srcV2IndxInPart2][k];
							if (!isValidNewEdge(srcV1, destV2, edgeVal1, edgeVal2)) {
								break;
							} else {
								if (!newEdgeExists) {

								}
							}
						}

					}

				}
			}
		}

	}

	/**
	 * Stores the new edges in the new edge arrays
	 */
	private void checkAndGenerateNewEdges() {

	}

	private void grammarcheck() {

	}
}
