package edu.uci.ics.cs.gdtc.computedpartprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.uci.ics.cs.gdtc.edgecomputer.NewEdgesList;
import edu.uci.ics.cs.gdtc.partitiondata.AllPartitions;
import edu.uci.ics.cs.gdtc.partitiondata.LoadedPartitions;
import edu.uci.ics.cs.gdtc.partitiondata.LoadedVertexInterval;
import edu.uci.ics.cs.gdtc.partitiondata.PartitionQuerier;
import edu.uci.ics.cs.gdtc.partitiondata.Vertex;
import edu.uci.ics.cs.gdtc.scheduler.SchedulerInfo;
import edu.uci.ics.cs.gdtc.support.GDTCLogger;
import edu.uci.ics.cs.gdtc.userinput.UserInput;

/**
 * 
 * @author Aftab <br>
 *         Created 30 October 2015
 */
public class ComputedPartProcessor {

	// private static PrintWriter[] partDegOutStrms;
	private static long partMaxPostNewEdges;
	private static final Logger logger = GDTCLogger.getLogger("graphdtc computedpartprocessor");

	/**
	 * Initializes the heuristic for maximum size of a partition after addition
	 * of new edges
	 */
	public static void initRepartitionConstraints() {
		int numParts = AllPartitions.getPartAllocTab().length;

		// get the total number of edges
		long numEdges = 0;
		long[] partSizes = SchedulerInfo.getPartSizes();
		for (int i = 0; i < partSizes.length; i++) {
			numEdges = numEdges + partSizes[i];
		}

		// average of edges by no. of partitions
		long avgEdgesPerPart = Math.floorDiv(numEdges, numParts);

		// the heuristic for interval max
		long partMax = (long) (avgEdgesPerPart * 0.9);

		// the heuristic for interval max after new edge addition
		long heuristic_newPartMax = (long) (partMax + partMax * 0.3);
		partMaxPostNewEdges = heuristic_newPartMax;
		System.out.println(partMaxPostNewEdges);
	}

	/**
	 * Update data structures based on the computed partitions
	 * 
	 * @param vertices
	 * @param newEdgesLL
	 * @param intervals
	 * @throws IOException
	 */
	public static void processParts(Vertex[] vertices, NewEdgesList[] newEdgesLL,
			ArrayList<LoadedVertexInterval> intervals) throws IOException {

		// TEST print
		// for (int i = 0; i < vertices.length; i++) {
		// if (vertices[i].getVertexId() == 36) {
		// System.out.println(vertices[i]);
		// System.out.println(newEdgesLL[i]);
		// }
		// }

		long[] partSizes = SchedulerInfo.getPartSizes();
		long edgeDestCount[][] = SchedulerInfo.getEdgeDestCount();

		// splitpoints
		ArrayList<Integer> splitPoints = new ArrayList<Integer>();
		ArrayList<Integer> splitVertices = new ArrayList<Integer>();
		TreeSet<Integer> newIntervals = new TreeSet<Integer>();
		TreeSet<Integer> newParts = new TreeSet<Integer>();

		int[][] loadPartOutDegs = LoadedPartitions.getLoadedPartOutDegs();

		// System.out.println("Look Here !! ! " +
		// loadPartOutDegs[0][PartitionQuerier.getPartArrIdFrmActualId(36, 1)]);

		/*
		 * Scanning each loaded partition, updating info, adding repartitioning
		 * split points
		 */

		for (int a = 0; a < intervals.size(); a++) {

			LoadedVertexInterval part = intervals.get(a);
			int partId = part.getPartitionId();
			int nodeDestVs[];
			int numOfNodeVertices = 0;
			int destPartId;
			int src;

			logger.info("Processing partition: " + partId + ".");

			System.out.println("Processing Partition: " + partId);
			System.out.println("First Vertex: " + part.getFirstVertex());
			System.out.println("Last Vertex: " + part.getLastVertex());
			System.out.println("First Vertex Index: " + part.getIndexStart());
			System.out.println("Last Vertex Index: " + part.getIndexEnd());

			// get partition's indices in "vertices" data structure
			int partStart = part.getIndexStart();
			int partEnd = part.getIndexEnd();

			// 1. Scan the new edges and update partSizes, loadPartOutDegs, and
			// edgeDestCounts

			// for each src vertex
			for (int i = partStart; i < partEnd + 1; i++) {

				// get the actual source id
				src = i - partStart + part.getFirstVertex();

				// src extraction test
				// System.out.println("Index of Src in DataStructure" + i);
				// System.out.println("Actual Id of Src" + src);

				// if a new edge for this source exits
				if (newEdgesLL[i] != null) {

					// for each new edge list node
					for (int j = 0; j < newEdgesLL[i].getSize(); j++) {

						numOfNodeVertices = newEdgesLL[i].getNode(j).getIndex();
						nodeDestVs = newEdgesLL[i].getNode(j).getDstVertices();

						// update degrees
						partSizes[partId] += numOfNodeVertices;
						loadPartOutDegs[a][PartitionQuerier.getPartArrIdFrmActualId(src, partId)] += numOfNodeVertices;

						// for each dest vertex
						for (int k = 0; k < numOfNodeVertices; k++) {

							// update edgeDestCount
							destPartId = PartitionQuerier.findPartition(nodeDestVs[k]);
							if (destPartId != -1) {
								edgeDestCount[partId][destPartId]++;
							}
						}
					}
				}
			}

			// 2. Add repartitioning split points

			// keeps track of the size of the current partition
			long partEdgeCount = 0;

			// for each src vertex
			for (int i = partStart; i < partEnd + 1; i++) {

				// get the actual source id
				src = i - partStart + part.getFirstVertex();

				partEdgeCount += loadPartOutDegs[a][PartitionQuerier.getPartArrIdFrmActualId(src, partId)];

				// if the size of the current partition has become
				// larger than the limit, this partition is split, and
				// thus, we add the id of this source vertex as a split
				// point
				if (partEdgeCount > partMaxPostNewEdges & i != partEnd) {
					splitPoints.add(i);
					splitVertices.add(src);
					partEdgeCount = 0;
				}
			}
		}

		/*
		 * Creating new partitions based on split points
		 */

		// testing PartitionQuerier before changing PAT 1/2
		// System.out.println("testing findpartition 13:" +
		// PartitionQuerier.findPartition(13));
		// System.out.println("testing getActualIdFrmPartArrId 4th vertex in
		// partition 0:"
		// + PartitionQuerier.getActualIdFrmPartArrId(4, 0));
		// System.out.println("testing getMaxSrc part 2:" +
		// PartitionQuerier.getMaxSrc(2));
		// System.out.println("testing getMinsSrc part 0:" +
		// PartitionQuerier.getMinSrc(0));
		// System.out.println("testing getnumunique sources part 2:" +
		// PartitionQuerier.getNumUniqueSrcs(2));
		// System.out.println(
		// "testing getPartArrIdFrmActualId src 38, part 2:" +
		// PartitionQuerier.getPartArrIdFrmActualId(38, 2));

		// 1. Updating partition allocation table
		for (int i = 0; i < splitVertices.size(); i++) {
			newIntervals.add(splitVertices.get(i));
		}

		int[][] partAllocTable = AllPartitions.getPartAllocTab();

		// add original intervals
		for (int i = 0; i < partAllocTable.length; i++) {
			newIntervals.add(partAllocTable[i][1]);
		}

		// initialize newPartAllocTable
		int[][] newPartAllocTable = new int[newIntervals.size()][2];
		for (int i = 0; i < newPartAllocTable.length; i++) {
			newPartAllocTable[i][0] = -1;
			newPartAllocTable[i][1] = -1;
		}

		// get the intervals in the new partition allocation table
		int c = 0;
		for (Integer i : newIntervals) {
			newPartAllocTable[c][1] = i;
			c++;
		}

		// get the partition ids from old PAT to new PAT
		int oldIntervalMax = 0, oldIntervalMin = 0;
		for (int i = 0; i < newPartAllocTable.length; i++) {
			for (int j = 0; j < partAllocTable.length; j++) {
				if (j == 0) {
					oldIntervalMin = 1;
				} else {
					oldIntervalMin = partAllocTable[j - 1][1] + 1;
				}
				oldIntervalMax = partAllocTable[j][1];
				if (newPartAllocTable[i][1] >= oldIntervalMin & newPartAllocTable[i][1] <= oldIntervalMax) {
					newPartAllocTable[i][0] = partAllocTable[j][0];
					partAllocTable[j][0] = -1;
				}
			}
		}

		// generate new partition ids
		int newPartId = partAllocTable.length;
		for (int i = 0; i < newPartAllocTable.length; i++) {
			if (newPartAllocTable[i][0] == -1) {
				newPartAllocTable[i][0] = newPartId;
				newPartId++;
			}
		}

		AllPartitions.setPartAllocTab(newPartAllocTable);
		UserInput.setNumParts(newPartAllocTable.length);

		// testing PartitionQuerier after changing PAT 2/2
		// System.out.println("testing findpartition 7:" +
		// PartitionQuerier.findPartition(7));
		// System.out.println("testing getActualIdFrmPartArrId 2nd vertex in
		// partition 5:"
		// + PartitionQuerier.getActualIdFrmPartArrId(2, 5));
		// System.out.println("testing getMaxSrc part 4:" +
		// PartitionQuerier.getMaxSrc(4));
		// System.out.println("testing getMinsSrc part 6:" +
		// PartitionQuerier.getMinSrc(6));
		// System.out.println("testing getnumunique sources part 3:" +
		// PartitionQuerier.getNumUniqueSrcs(3));
		// System.out.println(
		// "testing getPartArrIdFrmActualId src 8, part 4:" +
		// PartitionQuerier.getPartArrIdFrmActualId(8, 4));

		// print new partitions test code
		for (int i = 0; i < newPartAllocTable.length; i++) {
			System.out.println(newPartAllocTable[i][0] + " " + newPartAllocTable[i][1]);
		}


		// updating loaded partitions based on partition reload strategy
		int[] loadedParts = LoadedPartitions.getLoadedParts();
		if (UserInput.getPartReloadStrategy().compareTo("RELOAD_STRATEGY_2") == 0) {
			// once a partition is repartitioned, we don't consider it loaded.
			for (int i = 0; i < splitVertices.size(); i++) {
				for (int j = 0; j < loadedParts.length; j++) {
					if (loadedParts[j] == PartitionQuerier.findPartition(splitVertices.get(i))) {
						loadedParts[j] = Integer.MIN_VALUE;
						break;
					}
				}
			}
		}

		// System.out.println("Look Here !! ! " +
		// loadPartOutDegs[0][PartitionQuerier.getPartArrIdFrmActualId(36, 1)]);
	}

}
