package edu.uci.ics.cs.gdtc.engine;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.uci.ics.cs.gdtc.engine.data.AllPartitions;
import edu.uci.ics.cs.gdtc.engine.data.LoadedPartitions;
import edu.uci.ics.cs.gdtc.engine.support.Optimizers;
import edu.uci.ics.cs.gdtc.engine.support.PartitionQuerier;

/**
 * This program generates loads partitions into the memory and computes new
 * edges.
 * 
 * @author Aftab
 *
 */
public class PartitionLoader {

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
	public void loadParts(String baseFilename, int[] partsToLoad, int numParts) throws IOException {

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
		System.out.println("Done");

		// fill the partition data structures
		fillPartitionDataStructs(baseFilename, partsToLoad);
		System.out.println("Completed loading of partitions");

		int loadedPartOutDegrees[][] = LoadedPartitions.getLoadedPartOutDegs();
		int partEdgeArrays[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeValArrays[][][] = LoadedPartitions.getLoadedPartEdgeVals();

		// sorting the partitions
		System.out.print("Sorting loaded partition data structures... ");
		for (int i = 0; i < partsToLoad.length; i++) {
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partsToLoad[i]); j++) {
				int low = 0;
				int high = loadedPartOutDegrees[i][j] - 1;
				Optimizers.quickSort(partEdgeArrays[i][j], partEdgeValArrays[i][j], low, high);
			}
		}
		System.out.println("Done");

		LoadedPartitions.setLoadedParts(partsToLoad);

		/*
		 * TEST Loading of Partitions in Arrays
		 */
//		for (int i = 0; i < partsToLoad.length; i++) {
//			System.out.println("Partition: " + partsToLoad[i]);
//			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partsToLoad[i]); j++) {
//				int srcv = j + PartitionQuerier.getMinSrc(partsToLoad[i]);
//				System.out.println("SourceV: " + srcv);
//				System.out.println("Dest Vs: ");
//				for (int k = 0; k < loadedPartOutDegrees[i][j]; k++) {
//					System.out.print(partEdgeArrays[i][j][k] + " ");
//				}
//				System.out.println();
//				System.out.println("Edge Vals: ");
//				for (int k = 0; k < loadedPartOutDegrees[i][j]; k++) {
//					System.out.print(partEdgeValArrays[i][j][k] + " ");
//				}
//				System.out.println();
//			}
//		}
	}

	/**
	 * 
	 * Gets the partition allocation table.
	 * 
	 * @param baseFilename
	 * @param numParts
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void readPartAllocTable(String baseFilename, int numParts) throws NumberFormatException, IOException {

		// initialize partAllocTable variable
		int partAllocTable[] = new int[numParts];

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
		AllPartitions.setPartAllocTab(partAllocTable);
		System.out.println("Done");

	}

	/**
	 * Gets the degrees of the source vertices of the partitions that are to be
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
			partOutDegrees[i] = new int[PartitionQuerier.getNumUniqueSrcs(partitionsToLoad[i])];
		}

		/*
		 * Scan the degrees file
		 */
		BufferedReader outDegStream = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".degrees"))));

		System.out.print("Reading degrees file (" + baseFilename
				+ ".degrees) to obtain degrees of source vertices in partitions to load... ");

		String ln;
		while ((ln = outDegStream.readLine()) != null) {

			String[] tok = ln.split("\t");

			// get the srcVId and degree
			int srcVId = Integer.parseInt(tok[0]);
			int deg = Integer.parseInt(tok[1]);

			for (int i = 0; i < partitionsToLoad.length; i++) {

				// check if the srcVId belongs to this partition
				if (!PartitionQuerier.inPartition(srcVId, partitionsToLoad[i])) {
					continue;
				} else {
					try {
						partOutDegrees[i][srcVId - PartitionQuerier.getMinSrc(partitionsToLoad[i])] = deg;
					} catch (ArrayIndexOutOfBoundsException e) {
					}
				}
			}
		}
		LoadedPartitions.setLoadedPartOutDegs(partOutDegrees);
		outDegStream.close();
		System.out.println("Done");
	}

	/**
	 * Initializes data structures of the partitions to load.
	 */
	private void initPartitionDataStructs(int[] partitionsToLoad) {

		int[][] partOutDegrees = LoadedPartitions.getLoadedPartOutDegs();

		// initialize Dimension 1 (Total no. of Partitions)
		int partEdgeArrays[][][] = new int[partitionsToLoad.length][][];
		byte partEdgeValsArrays[][][] = new byte[partitionsToLoad.length][][];

		for (int i = 0; i < partitionsToLoad.length; i++) {

			// initialize Dimension 2 (Total no. of Unique SrcVs for a
			// Partition)
			partEdgeArrays[i] = new int[PartitionQuerier.getNumUniqueSrcs(partitionsToLoad[i])][];
			partEdgeValsArrays[i] = new byte[PartitionQuerier.getNumUniqueSrcs(partitionsToLoad[i])][];

			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partitionsToLoad[i]); j++) {

				// initialize Dimension 3 (Total no. of Out-edges for a SrcV)
				partEdgeArrays[i][j] = new int[partOutDegrees[i][j]];
				partEdgeValsArrays[i][j] = new byte[partOutDegrees[i][j]];

				for (int k = 0; k < partOutDegrees[i][j]; k++) {

					// initialize each entry to -1
					partEdgeValsArrays[i][j][k] = -1;// TODO - check whether you
														// need it.
				}
			}
		}

		LoadedPartitions.setLoadedPartEdges(partEdgeArrays);
		LoadedPartitions.setLoadedPartEdgeVals(partEdgeValsArrays);

	}

	/**
	 * Reads the partition files and stores them in arrays.
	 * 
	 * @param partInputStream
	 * @throws IOException
	 */
	private void fillPartitionDataStructs(String baseFilename, int[] partitionsToLoad) throws IOException {

		int[][][] partEdgeArrays = LoadedPartitions.getLoadedPartEdges();
		byte[][][] partEdgeValArrays = LoadedPartitions.getLoadedPartEdgeVals();

		for (int i = 0; i < partitionsToLoad.length; i++) {

			DataInputStream partitionInputStream = new DataInputStream(
					new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partitionsToLoad[i])));

			// stores the position of last filled edge (destV) and the edge val
			// in partEdgesArrays and partEdgeValArrays for a a source vertex
			// for a partition
			int[] lastAddedEdgePos = new int[PartitionQuerier.getNumUniqueSrcs(partitionsToLoad[i])];
			for (int j = 0; j < lastAddedEdgePos.length; j++) {
				lastAddedEdgePos[j] = -1;
			}

			while (partitionInputStream.available() != 0) {// (true);
				try {
					// get srcVId
					int src = partitionInputStream.readInt();

					// get corresponding arraySrcVId of srcVId
					int arraySrcVId = src - PartitionQuerier.getMinSrc(partitionsToLoad[i]);

					// get count (number of destVs from srcV in the current list
					// of the partition file)
					int count = partitionInputStream.readInt();

					// get dstVId & edgeVal and store them in the corresponding
					// arrays
					for (int j = 0; j < count; j++) {

						// dstVId
						partEdgeArrays[i][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partitionInputStream
								.readInt();

						// edgeVal
						partEdgeValArrays[i][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partitionInputStream
								.readByte();

						// increment the last added position for this row
						lastAddedEdgePos[arraySrcVId]++;
					}

				} catch (Exception exception) {
					break;
				}
			}
			partitionInputStream.close();

			LoadedPartitions.setLoadedPartEdges(partEdgeArrays);
			LoadedPartitions.setLoadedPartEdgeVals(partEdgeValArrays);

		}
	}

}
