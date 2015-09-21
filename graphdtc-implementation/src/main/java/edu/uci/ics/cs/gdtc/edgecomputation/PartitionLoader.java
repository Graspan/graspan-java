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
public class PartitionLoader {

	int partAllocTable[] = PartitionGenerator.partitionAllocationTable;
	int part1EdgesArray[][];
	int part2EdgesArray[][];
	int part1EdgeValsArray[][];
	int part2EdgeValsArray[][];

	/**
	 * Loads the two partitions in the memory
	 * 
	 * @param baseFilename
	 * @param partitionPair
	 * @throws IOException
	 */
	public void loadPartition(String baseFilename, int[] partitionPair) throws IOException {

		int part1MaxSrc, part1MinSrc, part2MaxSrc, part2MinSrc, numOfUniqueSrcsPart1, numOfUniqueSrcsPart2;

		int part1Id = partitionPair[0];
		int part2Id = partitionPair[1];

		/*
		 * Get the degrees of the source vertices in the 2 partitions from the
		 * degrees file, and store those degrees in two arrays
		 */

		// find the max src of each partition
		part1MaxSrc = partAllocTable[part1Id];
		part2MaxSrc = partAllocTable[part2Id];

		// find the number of unique source vertices in each partition | IMP we
		// consider the vertex numbering of the input graph to start from 1 NOT
		// 0
		numOfUniqueSrcsPart1 = findNumOfUniqueSrcs(part1MaxSrc, part1Id);
		numOfUniqueSrcsPart2 = findNumOfUniqueSrcs(part2MaxSrc, part2Id);

		// find the min src of each partition
		part1MinSrc = part1MaxSrc - numOfUniqueSrcsPart1 + 1;
		part2MinSrc = part2MaxSrc - numOfUniqueSrcsPart2 + 1;

		// create the degree arrays by reading the degrees file
		int part1degs[] = new int[numOfUniqueSrcsPart1];
		int part2degs[] = new int[numOfUniqueSrcsPart2];
		BufferedReader outDegStream = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".degrees"))));
		String ln;
		while ((ln = outDegStream.readLine()) != null) {
			String[] tok = ln.split("\t");
			int src = Integer.parseInt(tok[0]);
			int deg = Integer.parseInt(tok[1]);
			if (belongsToPartition(src, part1MinSrc, part1MaxSrc)) {
				part1degs[src - part1MinSrc] = deg;
				continue;
			}
			if (belongsToPartition(src, part2MinSrc, part2MaxSrc)) {
				part2degs[src - part2MinSrc] = deg;
				continue;
			}
		}

		// initialize two 2D arrays to store edges of the two partitions
		int[][] part1EdgesArray = new int[numOfUniqueSrcsPart1][];
		int[][] part2EdgesArray = new int[numOfUniqueSrcsPart2][];
		initialize2DPartArray(part1EdgesArray, numOfUniqueSrcsPart1, part1degs);
		initialize2DPartArray(part2EdgesArray, numOfUniqueSrcsPart2, part2degs);
		this.part1EdgesArray = part1EdgesArray;
		this.part2EdgesArray = part2EdgesArray;

		// initialize two 2D arrays to store the corresponding edge values of
		// the two partitions
		int[][] part1EdgeValsArray = new int[numOfUniqueSrcsPart1][];
		int[][] part2EdgeValsArray = new int[numOfUniqueSrcsPart2][];
		initialize2DPartArray(part1EdgeValsArray, numOfUniqueSrcsPart1, part1degs);
		initialize2DPartArray(part2EdgeValsArray, numOfUniqueSrcsPart2, part2degs);
		this.part1EdgeValsArray = part1EdgeValsArray;
		this.part2EdgeValsArray = part2EdgeValsArray;

		// create input streams of the two partitions
		// TODO likely to require translation of the ids - due to repartitioning
		DataInputStream part1InputStream = new DataInputStream(
				new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + part1Id)));
		DataInputStream part2InputStream = new DataInputStream(
				new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + part2Id)));

		// read the input streams into the corresponding arrays
		fillPartitionArray(part1InputStream, part1EdgesArray, part1EdgeValsArray, part1MinSrc);
		fillPartitionArray(part2InputStream, part2EdgesArray, part2EdgeValsArray, part2MinSrc);

		/*
		 * TEST Loading of Partitions in Arrays
		 */
		for (int i = 0; i < part2EdgeValsArray.length; i++) {
			for (int j = 0; j < part2degs[i]; j++) {
				System.out.print(part2EdgeValsArray[i][j] + " ");
			}
			System.out.println();
		}
		for (int i = 0; i < part1EdgesArray.length; i++) {
			for (int j = 0; j < part1degs[i]; j++) {
				System.out.print(part1EdgesArray[i][j] + " ");
			}
			System.out.println();
		}

	}

	/**
	 * Finds the number of unique sources in a partition (called by
	 * loadPartition method)
	 * 
	 * @param partMaxSrc
	 * @param partId
	 * @return
	 */
	private int findNumOfUniqueSrcs(int partMaxSrc, int partId) {
		int NumOfUniqueSrcs;
		if (partId == 0) {
			NumOfUniqueSrcs = partMaxSrc;
		} else {
			NumOfUniqueSrcs = partMaxSrc - partAllocTable[partId - 1];
		}
		return NumOfUniqueSrcs;
	}

	/**
	 * Finds whether the given vertex belongs to a partition
	 * 
	 * @param src
	 * @param partMinSrc
	 * @param partMaxSrc
	 * @return
	 */
	private boolean belongsToPartition(int src, int partMinSrc, int partMaxSrc) {
		if (src >= partMinSrc & src <= partMaxSrc)
			return true;
		else
			return false;
	}

	/**
	 * Initializes a 2D Partition Array (which corresponds to an adjacency list)
	 * 
	 * @param numOfUniqueSrcs
	 * @param partArray
	 * @param partDegs
	 */
	private void initialize2DPartArray(int[][] partArray, int numOfUniqueSrcs, int[] partDegs) {
		for (int i = 0; i < numOfUniqueSrcs; i++) {
			partArray[i] = new int[partDegs[i]];
		}
		for (int i = 0; i < numOfUniqueSrcs; i++) {
			for (int j = 0; j < partDegs[i]; j++) {
				partArray[i][j] = -1;
			}
		}
	}

	/**
	 * Reads the partition files and stores them in arrays
	 * 
	 * @param partInputStream
	 * @throws IOException
	 */
	private void fillPartitionArray(DataInputStream partInputStream, int[][] partEdgesArray, int[][] partEdgeValsArray,
			int partMinSrc) throws IOException {

		// initialize an array that stores the position of last added edge(dest
		// vert) and the edge val in partEdgesArray and partEdgeValsArray for a
		// given source vertex
		int[] lastAddedPositioninSrcRow = new int[partEdgesArray.length];
		for (int i = 0; i < partEdgesArray.length; i++) {
			lastAddedPositioninSrcRow[i] = -1;
		}

		while (true) {
			try {
				// get srcVId
				int src = partInputStream.readInt();

				// get corresponding arraySrcVId of srcVId
				int arraySrcVId = src - partMinSrc;

				// get count (number of destVs from srcV in the current list of
				// the partition file)
				int count = partInputStream.readInt();

				// get dstVId & edgeVal and store them in the corresponding
				// arrays
				for (int i = 0; i < count; i++) {

					// dstVId
					partEdgesArray[arraySrcVId][lastAddedPositioninSrcRow[arraySrcVId] + 1] = partInputStream.readInt();

					// edgeVal
					partEdgeValsArray[arraySrcVId][lastAddedPositioninSrcRow[arraySrcVId] + 1] = partInputStream
							.readByte();

					// increment the last added position for this row
					lastAddedPositioninSrcRow[arraySrcVId]++;
				}

			} catch (Exception exception) {
				break;
			}
		}
		partInputStream.close();
	}

}
