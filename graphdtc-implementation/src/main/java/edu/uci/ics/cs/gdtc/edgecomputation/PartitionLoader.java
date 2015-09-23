package edu.uci.ics.cs.gdtc.edgecomputation;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.uci.ics.cs.gdtc.preproc.PartitionGenerator;

/**
 * 
 * @author Aftab
 *
 */
public class PartitionLoader {

	private static final int NEW_EDGE_BUFFER_SIZE = 10;
	private static final int NEW_EDGE_INDEX_BUFFER_SIZE = 10;

	int partAllocTable[] = PartitionGenerator.partAllocTable;

	/*
	 * Data structures for storing the partitions to load
	 * 
	 * Dimension 1 indicates partition number, Dimension 2 indicates list for a
	 * source vertex, Dimension 3 indicates an out-edge from each source vertex
	 */
	public int partEdgeArrays[][][];
	public int partEdgeValArrays[][][];

	/*
	 * Stores the out degrees of each source vertex of each partition.
	 * 
	 * Dimension 1 indicates partition number, Dimension 2 indicates out degree
	 * of a source vertex in the partition indicated by Index 1
	 */
	public int partOutDegrees[][];

	// public DataInputStream partitionInputStreams[];|| USE IF USING MULTI
	// THREADING

	/*
	 * New Edge Data Structures
	 */
	// int part1newEdges[] = new int[NEW_EDGE_BUFFER_SIZE];
	// int part2newEdges[] = new int[NEW_EDGE_BUFFER_SIZE];
	// int part1newEdgeVals[] = new int[NEW_EDGE_BUFFER_SIZE];
	// int part2newEdgeVals[] = new int[NEW_EDGE_BUFFER_SIZE];
	// int part1newEdgeIndex[] = new int[NEW_EDGE_INDEX_BUFFER_SIZE];
	// int part2newEdgeIndex[] = new int[NEW_EDGE_INDEX_BUFFER_SIZE];
	// int part1lastAddedEdgePos, part2lastAddedEdgePos;

	/**
	 * Loads the two partitions in the memory
	 * 
	 * @param baseFilename
	 * @param partitionPair
	 * @throws IOException
	 */
	public void loadPartition(String baseFilename, int[] partitionsToLoad) throws IOException {

		// get the degrees of the source vertices in the partitions
		getDegrees(baseFilename, partitionsToLoad);

		// initialize data structures of the partitions to load
		initPartitionDataStructs(partitionsToLoad);

		// fill the partition data structures
		fillPartitionDataStructs(baseFilename, partitionsToLoad);

		/*
		 * TEST Loading of Partitions in Arrays
		 */
		for (int i = 0; i < partitionsToLoad.length; i++) {
			System.out.println("Partition: " + partitionsToLoad[i]);
			for (int j = 0; j < this.getNumUniqueSrcs(partitionsToLoad[i]); j++) {
				int srcv = j + this.getMinSrc(partitionsToLoad[i]);
				System.out.println("SourceV: " + srcv);
				System.out.println("Edge Vals: ");
				for (int k = 0; k < this.partOutDegrees[i][j]; k++) {
					System.out.print(this.partEdgeValArrays[i][j][k] + " ");
				}
				System.out.println();
			}
		}
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
					try {
						partOutDegrees[i][srcVId - getMinSrc(partitionsToLoad[i])] = deg;
					} catch (ArrayIndexOutOfBoundsException e) {
						System.out.println("hellp");
						System.out.println("Source " + srcVId);
						System.out.println("Partition Id in array " + i);
						System.out.println("The actual partition Id " + partitionsToLoad[i]);
						System.out.println(getMinSrc(partitionsToLoad[i]));
						System.out.println(partOutDegrees[1][3]);
						System.exit(0);
					}
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


}
