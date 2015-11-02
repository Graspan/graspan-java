package edu.uci.ics.cs.gdtc.computedpartprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import edu.uci.ics.cs.gdtc.edgecomputer.NewEdgesList;
import edu.uci.ics.cs.gdtc.partitiondata.AllPartitions;
import edu.uci.ics.cs.gdtc.partitiondata.LoadedPartitions;
import edu.uci.ics.cs.gdtc.partitiondata.LoadedVertexInterval;
import edu.uci.ics.cs.gdtc.partitiondata.PartitionQuerier;
import edu.uci.ics.cs.gdtc.partitiondata.Vertex;
import edu.uci.ics.cs.gdtc.scheduler.SchedulerInfo;
import edu.uci.ics.cs.gdtc.support.GDTCLogger;

/**
 * 
 * @author Aftab 30 October 2015
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
	}

	/**
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

		int[][] loadPartOutDegs = LoadedPartitions.getLoadedPartOutDegs();
		int src;

		System.out.println("Look Here  !! ! " + loadPartOutDegs[0][PartitionQuerier.getPartArrIdFrmActualId(36, 1)]);

		/*
		 * Scanning each loaded partition and updating info
		 */
		for (int a = 0; a < intervals.size(); a++) {
			LoadedVertexInterval part = intervals.get(a);
			int partId = part.getPartitionId();
			int newEdgeList[];
			int destPartId;
			logger.info("Processing partition: " + partId + ".");
			// logger.info("First Vertex: " + part.getFirstVertex() + ".");
			// logger.info("Last Vertex: " + part.getLastVertex() + ".");

			// get partition's indices in "vertices" data structure
			int partStart = part.getIndexStart();
			int partEnd = part.getIndexEnd();

			long partSize;

			/*
			 * update partSizes and edgeDestCounts
			 */
			// for each src vertex
			for (int i = partStart; i < partEnd + 1; i++) {
				src = i - partStart + part.getFirstVertex();
				if (newEdgesLL[i] != null) {
					// for each newedgelistnode
					for (int j = 0; j < newEdgesLL[i].getSize(); j++) {
						newEdgeList = newEdgesLL[i].getNode(j).getDstVertices();
						// for each dest vertex
						for (int k = 0; k < newEdgeList.length; k++) {
							if (newEdgeList[k] != 0) {
								partSizes[partId]++;
								loadPartOutDegs[a][PartitionQuerier.getPartArrIdFrmActualId(src, partId)]++;
								destPartId = PartitionQuerier.findPartition(newEdgeList[k]);
								if (destPartId != -1) {
									edgeDestCount[partId][destPartId]++;
								}
							}
						}
					}
					partSize = partSizes[partId];
					if (partSize > partMaxPostNewEdges & i != partEnd) {
						splitPoints.add(i);
						partSize = 0;
					}
				}
			}
		}

		// System.out.println("Look Here !! ! " +
		// loadPartOutDegs[0][PartitionQuerier.getPartArrIdFrmActualId(36, 1)]);
	}

}
