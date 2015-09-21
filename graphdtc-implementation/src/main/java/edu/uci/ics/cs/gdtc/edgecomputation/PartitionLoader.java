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

		int part1Id = partitionPair[0];
		int part2Id = partitionPair[1];

		/*
		 * Get the degrees of the source vertices in the 2 partitions from the
		 * degrees file, and store those degrees in two arrays
		 */

		// find the max src of each partition
		int part1MaxSrc = partAllocTable[part1Id];
		int part2MaxSrc = partAllocTable[part2Id];

		// find the number of unique source vertices in each partition | IMP we
		// consider the vertex numbering of the input graph to start from 1 NOT
		// 0
		int numOfUniqueSrcsPart1 = findNumOfUniqueSrcs(part1MaxSrc, part1Id);
		int numOfUniqueSrcsPart2 = findNumOfUniqueSrcs(part2MaxSrc, part2Id);

		// find the min src of each partition
		int part1MinSrc = part1MaxSrc - numOfUniqueSrcsPart1 + 1;
		int part2MinSrc = part2MaxSrc - numOfUniqueSrcsPart2 + 1;

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
		DataInputStream part1InputStream = new DataInputStream(
				new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + part1Id)));
		DataInputStream part2InputStream = new DataInputStream(
				new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + part2Id)));

		// read the input streams into the corresponding arrays
		fillPartitionArray(part1InputStream);
		fillPartitionArray(part2InputStream);

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
	private void fillPartitionArray(DataInputStream partInputStream) throws IOException {
		while (true) {
			try {
				// srcVId
				int src = partInputStream.readInt();
				// System.out.println("src " + partInputStream.readInt());
				// System.out.println("-----");

				// count (number of destVs from srcV in the current list of the
				// partition file)
				int count = partInputStream.readInt();
				// System.out.println("# " + count);
				// System.out.println("-----");

				for (int i = 0; i < count; i++) {
					// dstVId
					System.out.println("dst " + partInputStream.readInt());

					// edge value
					System.out.println("val " + partInputStream.readByte());

				}
				System.out.println("=====");

			} catch (Exception exception) {
				break;
			}
		}
		partInputStream.close();
	}

}
