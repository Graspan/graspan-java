package edu.uci.ics.cs.gdtc.edgecomputation;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.cs.gdtc.Vertex;
import edu.uci.ics.cs.gdtc.engine.LoadedVertexInterval;
import edu.uci.ics.cs.gdtc.preproc.PartitionGenerator;

/**
 * This program generates loads partitions into the memory and computes new
 * edges.
 * 
 * @author Aftab
 *
 */
public class PartitionLoader {

	/*
	 * TODO Remember to update partAllocTable after repartitioning and reset all
	 * the other data structures after the end of a new edge computation
	 * iteration
	 */
	private int partAllocTable[] = PartitionGenerator.partAllocTable;

	// Data structures for storing the partitions to load:
	// Dimension 1 indicates partition number, Dimension 2 indicates list for a
	// source vertex, Dimension 3 indicates an out-edge from each source vertex
	private int partEdgeArrays[][][];
	private byte partEdgeValArrays[][][];

	// Stores the out degrees of each source vertex of each partition.
	// Dimension 1 indicates partition number, Dimension 2 indicates out degree
	// of a source vertex in the partition indicated by Index 1
	private int partOutDegrees[][];

	// Contains the ids of the partitions to be loaded in the memory
	private int loadedParts[];

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

	private Vertex[] vertices = null;
	private List<LoadedVertexInterval> intervals = new ArrayList<LoadedVertexInterval>();
	/**
	 * PART 1: LOADING PHASE
	 */

	/**
	 * Loads the two partitions in the memory
	 * 
	 * @param baseFilename
	 * @param partitionPair
	 * @throws IOException
	 */
	public void loadPartitions(String baseFilename, int[] partsToLoad, int numParts) throws IOException {

		System.out.print("Loading partitions: ");
		for (int i = 0; i < partsToLoad.length; i++) {
			System.out.print(partsToLoad[i] + " ");
		}
		System.out.print("\n");
		
		// get the partition allocation table
		readPartAllocTable(baseFilename, numParts);
				
		// get the degrees of the source vertices in the partitions
		getDegrees(baseFilename, partsToLoad);

		// initialize data structures of the partitions to load
		System.out.print("Initializing data structures for loading partitions... ");
		initPartitionDataStructs(partsToLoad);
		System.out.print("Done\n");

		// fill the partition data structures
		fillPartitionDataStructs(baseFilename, partsToLoad);
		System.out.println("Completed loading of partitions");

		// sorting the partitions
//		System.out.print("Sorting loaded partition data structures... ");
//		for (int i = 0; i < partsToLoad.length; i++) {
//			for (int j = 0; j < this.getNumUniqueSrcs(partsToLoad[i]); j++) {
//				int low = 0;
//				int high = this.partOutDegrees[i][j] - 1;
//				quickSort(partEdgeArrays[i][j], partEdgeValArrays[i][j], low, high);
//			}
//		}
		System.out.print("Done\n");

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
	 * 
	 * Gets the partition allocation table
	 * 
	 * @param baseFilename
	 * @param numParts
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void readPartAllocTable(String baseFilename, int numParts) throws NumberFormatException, IOException {

		// initialize partAllocTable variable
		partAllocTable = new int[numParts];

		/*
		 * Scan the partition allocation table file
		 */
		BufferedReader outPartAllocTabStream = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".partAllocTable"))));
		String ln;

		System.out.print("Reading partition allocation table file " + baseFilename + ".partAllocTable... ");
		int i = 0;
		while ((ln = outPartAllocTabStream.readLine()) != null) {
			String tok = ln;
			// store partition allocation table in memory
			partAllocTable[i] = Integer.parseInt(tok);
			i++;
		}
//		AllPartitions.setPartAllocTab(partAllocTable);
		System.out.println("Done");

	}
	
	public Vertex[] getVertices() {
		return vertices;
	}
	
	public List<LoadedVertexInterval> getIntervals() {
		return intervals;
	}

	/**
	 * (Called by loadPartitions method()) Rendered from:
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

		System.out.print("Reading degrees file to obtain degrees of source vertices in partitions... ");
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
					try {
						partOutDegrees[i][srcVId - getMinSrc(partitionsToLoad[i])] = deg;
					} catch (ArrayIndexOutOfBoundsException e) {
						// System.out.println("hellp");
						// System.out.println("Source " + srcVId);
						// System.out.println("Partition Id in array " + i);
						// System.out.println("The actual partition Id " + 1);
						// System.out.println("Min Src of this partition: " +
						// getMinSrc(0));
						// System.out.println("Max Src of this partition: " +
						// getMaxSrc(4));
						// System.out.println("num of uniq sources: " +
						// this.getNumUniqueSrcs(1));
						System.out.println("Min: " + getMinSrc(1));
						// int a = srcVId - getMinSrc(partitionsToLoad[i]);
						// System.out.println("Problem at: " + a);
						// System.out.println(partOutDegrees[0][0]);
						System.exit(0);
					}
				}
			}
		}
		this.partOutDegrees = partOutDegrees;
		outDegStream.close();
		System.out.print("Done\n");
	}

	/**
	 * Initializes data structures of the partitions to load
	 */
	private void initPartitionDataStructs(int[] partitionsToLoad) {

		// initialize Dimension 1 (Total no. of Partitions)
		int partEdgeArrays[][][] = new int[partitionsToLoad.length][][];
		byte partEdgeValsArrays[][][] = new byte[partitionsToLoad.length][][];
		
		int totalNumVertices = 0;
		for(int i = 0; i < partitionsToLoad.length; i++) 
			totalNumVertices += getNumUniqueSrcs(partitionsToLoad[i]);
		
		vertices = new Vertex[totalNumVertices];
		
		int count = 0;
		for (int i = 0; i < partitionsToLoad.length; i++) {

			// initialize Dimension 2 (Total no. of Unique SrcVs for a
			// Partition)
			partEdgeArrays[i] = new int[this.getNumUniqueSrcs(partitionsToLoad[i])][];
			partEdgeValsArrays[i] = new byte[this.getNumUniqueSrcs(partitionsToLoad[i])][];
			
			for (int j = 0; j < this.getNumUniqueSrcs(partitionsToLoad[i]); j++) {

				// initialize Dimension 3 (Total no. of Out-edges for a SrcV)
				partEdgeArrays[i][j] = new int[this.partOutDegrees[i][j]];
				partEdgeValsArrays[i][j] = new byte[this.partOutDegrees[i][j]];
				
				int vertexId = getActualIdFrmPartArrId(j, partitionsToLoad[i]); 
				
				vertices[count++] =  new Vertex(vertexId, 
						partEdgeArrays[i][j], partEdgeValsArrays[i][j]);
						
				for (int k = 0; k < this.partOutDegrees[i][j]; k++) {

					// initialize each entry to -1
					partEdgeValsArrays[i][j][k] = -1;// TODO - check whether you
														// need it.
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
		int indexSt = 0;
		int indexEd = 0;
		for (int i = 0; i < partitionsToLoad.length; i++) {

			DataInputStream partitionInputStream = new DataInputStream(
					new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partitionsToLoad[i])));

			// stores the position of last filled edge (destV) and the edge val
			// in partEdgesArrays and partEdgeValArrays for a a source vertex
			// for a partition
			int[] lastAddedEdgePos = new int[getNumUniqueSrcs(partitionsToLoad[i])];
			for (int j = 0; j < lastAddedEdgePos.length; j++) {
				lastAddedEdgePos[j] = -1;
			}

			while (true) {// (partitionInputStream.available()!=0);
				try {
					// get srcVId
					int src = partitionInputStream.readInt();// src=b;

					// get corresponding arraySrcVId of srcVId
					int arraySrcVId = src - getMinSrc(partitionsToLoad[i]);

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
			
			int partitionId = partitionsToLoad[i];
			LoadedVertexInterval interval = new LoadedVertexInterval(getMinSrc(partitionId), 
					getMaxSrc(partitionId), partitionId);
			interval.setIndexStart(indexSt);
			indexEd = indexEd +  getNumUniqueSrcs(partitionId) - 1;
			interval.setIndexEnd(indexEd);
			intervals.add(interval);
			indexSt = indexEd + 1;
			partitionInputStream.close();
		}
		
		assert(intervals.size() == 2);
	}

	/**
	 * PART 2: COMPUTATION OF NEW EDGES
	 */

	/**
	 * INCOMPLETE
	 */
	public void computeNewEdges() {

		initNewEdgeDataStructs();

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

		// do repartitioning(if needed), if all threads have finished.
		// then write the partitions to disk
		// then reset the data structures

	}

	/**
	 * Initializes the new edge data structures (Is called only once for one set
	 * of loaded partitions)
	 */
	public void initNewEdgeDataStructs() {
		newEdgeArraySet = new int[NUM_NEW_EDGE_ARRAY_SETS][SIZEOF_NEW_EDGE_ARRAY_SET];
		newEdgeArrSetStatus = new int[NUM_NEW_EDGE_ARRAY_SETS];
		newEdgeArrMarkersforSrc = new int[loadedParts.length][][][][];
	};

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
	 * PART 3 (SUPPLEMENTARY): METHODS FOR RETRIEVING MISCELLANEOUS INFO OF
	 * LOADED PARTITIONS (USED BY ALL PHASES)
	 */

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
			return (getMaxSrc(partId - 1) + 1);
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
	 * @param srcVId
	 * @param partId
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
