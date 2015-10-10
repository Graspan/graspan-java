package edu.uci.ics.cs.gdtc.edgecomputation;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.uci.ics.cs.gdtc.preproc.PartitionGenerator;

/**
 * 
 * @author Aftab
 *
 */
public class NewEdgeComputer {

	private int partAllocTable[] = PartitionGenerator.partAllocTable;

	/*
	 * Data structures for storing the partitions to load
	 * 
	 * Dimension 1 indicates partition number, Dimension 2 indicates list for a
	 * source vertex, Dimension 3 indicates an out-edge from each source vertex
	 */
	private int partEdgeArrays[][][];
	private int partEdgeValArrays[][][];

	/*
	 * Stores the out degrees of each source vertex of each partition.
	 * 
	 * Dimension 1 indicates partition number, Dimension 2 indicates out degree
	 * of a source vertex in the partition indicated by Index 1
	 */
	private int partOutDegrees[][];

	private int loadedParts[];

	/*
	 * New Edge Data Structures
	 */
	private static final int NEW_EDGE_BUFFER_SIZE = 10;
	private static final int NEW_EDGE_INDEX_BUFFER_SIZE = 10;
	int newEdges[] = new int[NEW_EDGE_BUFFER_SIZE];
	int newEdgeVals[] = new int[NEW_EDGE_BUFFER_SIZE];
	int newEdgeIndex[][];
	int lastAddedEdgePos;

	/**
	 * Loads the two partitions in the memory
	 * 
	 * @param baseFilename
	 * @param partitionPair
	 * @throws IOException
	 */
	public void loadPartitions(String baseFilename, int[] partsToLoad) throws IOException {

		// get the degrees of the source vertices in the partitions
		getDegrees(baseFilename, partsToLoad);

		// initialize data structures of the partitions to load
		initPartitionDataStructs(partsToLoad);

		// fill the partition data structures
		fillPartitionDataStructs(baseFilename, partsToLoad);

		
		// sorting the partitions
		for (int i = 0; i < partsToLoad.length; i++) {
			for (int j = 0; j < this.getNumUniqueSrcs(partsToLoad[i]); j++) {
				int low = 0;
				int high = this.partOutDegrees[i][j] - 1;
				quickSort(partEdgeArrays[i][j], partEdgeValArrays[i][j], low, high);
			}
		}

		this.loadedParts = partsToLoad;

		/*
		 * TEST Loading of Partitions in Arrays
		 */
		for (int i = 0; i < partsToLoad.length; i++) {
			System.out.println("Partition: " + partsToLoad[i]);
			for (int j = 0; j < this.getNumUniqueSrcs(partsToLoad[i]); j++) {
				int srcv = j + this.getMinSrc(partsToLoad[i]);
				System.out.println("SourceV: " + srcv);
				System.out.println("Dest Vs: ");
				for (int k = 0; k < this.partOutDegrees[i][j]; k++) {
					System.out.print(this.partEdgeArrays[i][j][k] + " ");
				}
				System.out.println();
				System.out.println("Edge Vals: ");
				for (int k = 0; k < this.partOutDegrees[i][j]; k++) {
					System.out.print(this.partEdgeValArrays[i][j][k] + " ");
				}
				System.out.println();
			}
		}
	}

	/**
	 * Rendered from:
	 * http://www.programcreek.com/2012/11/quicksort-array-in-java/ accessed at
	 * 9 October 2015
	 * 
	 * For sorting each source vertex adjacency list in the loaded partitions
	 * 
	 * @param arr
	 * @param low
	 * @param high
	 */
	public static void quickSort(int[] edgeArr, int[] edgeValArr, int low, int high) {
		if (edgeArr == null || edgeArr.length == 0)
			return;

		if (low >= high)
			return;

		// pick the pivot
		int middle = low + (high - low) / 2;
		int pivot = edgeArr[middle];

		// make left < pivot and right > pivot
		int i = low, j = high;
		while (i <= j) {
			while (edgeArr[i] < pivot) {
				i++;
			}

			while (edgeArr[j] > pivot) {
				j--;
			}

			if (i <= j) {

				int temp = edgeArr[i];
				edgeArr[i] = edgeArr[j];
				edgeArr[j] = temp;

				temp = edgeValArr[i];
				edgeValArr[i] = edgeValArr[j];
				edgeValArr[j] = temp;

				i++;
				j--;

			}
		}

		// recursively sort two sub parts
		if (low < j)
			quickSort(edgeArr, edgeValArr, low, j);

		if (high > i)
			quickSort(edgeArr, edgeValArr, i, high);
	}

	public void computeNewEdges() {

		// initialize the new edge index
		initNewEdgeIdx();

		// scan each partition
		for (int i = 0; i < loadedParts.length; i++) {
			for (int j = 0; j < this.getNumUniqueSrcs(loadedParts[i]); j++) {
				for (int k = 0; k < this.partOutDegrees[i][j]; k++) {

					// Edge 1
					int srcV1 = this.getActualIdFrmPartArrId(j, loadedParts[i]);
					int destV1 = this.partEdgeArrays[i][j][k];
					int edgeVal1 = this.partEdgeValArrays[i][j][k];

					// find whether the destination vertex belongs to any of the
					// loaded partitions as a source vertex
					for (int l = 0; l < loadedParts.length; l++) {
						// if the destination vertex does not belong to any of
						// the loaded partitions as a source vertex
						if (!inPartition(destV1, loadedParts[l])) {
							continue;
						}
						// else if the destination vertex belongs to any of the
						// loaded partitions as a source vertex
						else {
							int srcV2 = destV1;
							int srcV2ArrId = this.getPartArrIdFrmActualId(srcV2, loadedParts[l]);

							for (int m = 0; m < this.partOutDegrees[l][srcV2ArrId]; m++) {
								int destV2 = this.partEdgeArrays[l][srcV2ArrId][m];
								int edgeVal2 = this.partEdgeValArrays[l][srcV2ArrId][m];
								int newEdgeVal = generateNewEdgeVal(edgeVal1, edgeVal2);
								if (newEdgeVal != -1) {
									// get last occurrence
									this.getNewEdgeArrId(srcV1, i);
								}
								;
							}
						}
					}
				}
			}
		}

		// scan new edges
		// if new edges generated = false abort
		// new edge index
		// keep a counter of sizes of the partitions (an array)

	}

	/**
	 * Saves the degrees of the source vertices of the partitions that are to be
	 * loaded.
	 * 
	 * @param baseFilename
	 * @param partitionsToLoad
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	private void getDegrees(String baseFilename, int[] partitionsToLoad) throws NumberFormatException, IOException {

		/*
		 * Initialize the degrees array for each partition
		 */

		// initialize Dimension 1 (Total no. of Partitions)
		int[][] partOutDegrees = new int[partitionsToLoad.length][];

		for (int i = 0; i < partitionsToLoad.length; i++) {

			// initialize Dimension 2 (Total no. of Unique SrcVs for a
			// Partition)
			partOutDegrees[i] = new int[getNumUniqueSrcs(partitionsToLoad[i])];
		}

		/*
		 * Scan the degrees file
		 */
		BufferedReader outDegStream = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".degrees"))));
		String ln;

		while ((ln = outDegStream.readLine()) != null) {

			String[] tok = ln.split("\t");

			// get the srcVId and degree
			int srcVId = Integer.parseInt(tok[0]);
			int deg = Integer.parseInt(tok[1]);

			for (int i = 0; i < partitionsToLoad.length; i++) {

				// check if the srcVId belongs to this partition
				if (!inPartition(srcVId, partitionsToLoad[i])) {
					continue;
				} else {
					// try {
					partOutDegrees[i][srcVId - getMinSrc(partitionsToLoad[i])] = deg;
					// } catch (ArrayIndexOutOfBoundsException e) {
					// System.out.println("hellp");
					// System.out.println("Source " + srcVId);
					// System.out.println("Partition Id in array " + i);
					// System.out.println("The actual partition Id " +
					// partitionsToLoad[i]);
					// System.out.println(getMinSrc(partitionsToLoad[i]));
					// System.out.println(partOutDegrees[1][3]);
					// System.exit(0);
					// }
				}
			}
		}
		this.partOutDegrees = partOutDegrees;
		outDegStream.close();
	}

	/**
	 * Initializes data structures of the partitions to load
	 */
	private void initPartitionDataStructs(int[] partitionsToLoad) {

		// initialize Dimension 1 (Total no. of Partitions)
		int partEdgeArrays[][][] = new int[partitionsToLoad.length][][];
		int partEdgeValsArrays[][][] = new int[partitionsToLoad.length][][];

		for (int i = 0; i < partitionsToLoad.length; i++) {

			// initialize Dimension 2 (Total no. of Unique SrcVs for a
			// Partition)
			partEdgeArrays[i] = new int[this.getNumUniqueSrcs(partitionsToLoad[i])][];
			partEdgeValsArrays[i] = new int[this.getNumUniqueSrcs(partitionsToLoad[i])][];

			for (int j = 0; j < this.getNumUniqueSrcs(partitionsToLoad[i]); j++) {

				// initialize Dimension 3 (Total no. of Out-edges for a SrcV)
				partEdgeArrays[i][j] = new int[this.partOutDegrees[i][j]];
				partEdgeValsArrays[i][j] = new int[this.partOutDegrees[i][j]];

				for (int k = 0; k < this.partOutDegrees[i][j]; k++) {

					// initialize each entry to -1
					partEdgeValsArrays[i][j][k] = -1;
				}
			}
		}

		this.partEdgeArrays = partEdgeArrays;
		this.partEdgeValArrays = partEdgeValsArrays;

	}

	/**
	 * Reads the partition files and stores them in arrays
	 * 
	 * @param partInputStream
	 * @throws IOException
	 */
	private void fillPartitionDataStructs(String baseFilename, int[] partitionsToLoad) throws IOException {

		// // initialize partitionInputStreams array || USE IF USING
		// MULTI-THREADING
		// DataInputStream partitionInputStreams[] = new
		// DataInputStream[partitionsToLoad.length];

		for (int i = 0; i < partitionsToLoad.length; i++) {

			DataInputStream partitionInputStream = new DataInputStream(
					new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partitionsToLoad[i])));

			// stores the position of last filled edge (destV) and the edge val
			// in partEdgesArrays and partEdgeValArrays for a a source vertex
			// for a partition
			int[] lastAddedEdgePos = new int[this.getNumUniqueSrcs(partitionsToLoad[i])];
			for (int j = 0; j < lastAddedEdgePos.length; j++) {
				lastAddedEdgePos[j] = -1;
			}

			while (true) {
				try {
					// get srcVId
					int src = partitionInputStream.readInt();

					// get corresponding arraySrcVId of srcVId
					int arraySrcVId = src - this.getMinSrc(partitionsToLoad[i]);

					// get count (number of destVs from srcV in the current list
					// of the partition file)
					int count = partitionInputStream.readInt();

					// get dstVId & edgeVal and store them in the corresponding
					// arrays
					for (int j = 0; j < count; j++) {

						// dstVId
						this.partEdgeArrays[i][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partitionInputStream
								.readInt();

						// edgeVal
						this.partEdgeValArrays[i][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partitionInputStream
								.readByte();

						// increment the last added position for this row
						lastAddedEdgePos[arraySrcVId]++;
					}

				} catch (Exception exception) {
					break;
				}
			}
			partitionInputStream.close();
		}
	}

	/**
	 * Initializes the new edge index data structure
	 */
	private void initNewEdgeIdx() {
		this.newEdgeIndex = new int[loadedParts.length][];
		for (int i = 0; i < this.loadedParts.length; i++) {
			newEdgeIndex[i] = new int[this.getNumUniqueSrcs(this.loadedParts[i])];
		}
		for (int i = 0; i < loadedParts.length; i++) {
			for (int j = 0; j < this.getNumUniqueSrcs(this.loadedParts[i]); j++) {
				newEdgeIndex[i][j] = -1;
			}
		}
	}

	/**
	 * Reads the grammar info in the memory and returns a new production if
	 * derivable from the input values
	 * 
	 * @param edgeVal1
	 * @param edgeVal2
	 * @return
	 */
	private int generateNewEdgeVal(int edgeVal1, int edgeVal2) {
		int grammar[][] = new int[5][3];
		int newEdgeVal = -1;
		for (int i = 0; i < grammar.length; i++) {
			if (grammar[i][0] == edgeVal1 & grammar[i][1] == edgeVal2) {
				newEdgeVal = grammar[i][2];
				return newEdgeVal;
			}
		}
		return newEdgeVal;
	}

	/**
	 * Gets the id of the first occurence of the entry of a source vertex in the
	 * new edge data structures
	 * 
	 * @return
	 */
	private int getNewEdgeArrId(int srcV1, int partId) {

		// get the first entry position of this source vertex from the
		// newEdgeIndex array

		// first find the position of the source vertex in the given partition
		// int partArrId=getPartArrIdFrmActualId(partId)
		// firstId=newEdgeIndex[];
		return 0;

	}

	/**
	 * Returns the number of unique sources of in partition partId. IMP: we
	 * consider the vertex numbering of the input graph to start from 1 NOT 0
	 * 
	 * @param partId
	 */
	private int getNumUniqueSrcs(int partId) {
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
	private int getMinSrc(int partId) {
		if (partId == 0) {
			return 1;
		} else {
			return getMaxSrc(partId) - getMaxSrc(partId - 1) + 1;
		}
	}

	/**
	 * Returns the largest source vertex Id in partition partId
	 * 
	 * @param partId
	 * @return
	 */
	private int getMaxSrc(int partId) {
		return partAllocTable[partId];
	}

	/**
	 * Finds whether the given vertex belongs to a partition
	 * 
	 * @param src
	 * @param partMinSrc
	 * @param partAllocTable
	 * @return
	 */
	private boolean inPartition(int srcVId, int partId) {
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
	private int getActualIdFrmPartArrId(int vertexPartArrId, int partId) {
		return vertexPartArrId + this.getMinSrc(partId);
	}

	/**
	 * Returns the Id of the source vertex in the loaded partition array from
	 * the actual Id of the source vertex
	 * 
	 * @param vertexId
	 * @param partId
	 * @return
	 */
	private int getPartArrIdFrmActualId(int vertexId, int partId) {
		return this.getMaxSrc(partId) - vertexId;
	}

}
