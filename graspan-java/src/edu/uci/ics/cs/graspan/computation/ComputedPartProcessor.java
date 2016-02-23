package edu.uci.ics.cs.graspan.computation;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.GlobalParams;
import edu.uci.ics.cs.graspan.datastructures.LoadedPartitions;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.NewEdgesList;
import edu.uci.ics.cs.graspan.datastructures.PartitionQuerier;
import edu.uci.ics.cs.graspan.datastructures.RepartitioningData;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.scheduler.SchedulerInfo;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * 
 * @author Aftab <br>
 *         Created 30 October 2015
 */
public class ComputedPartProcessor {

	private static long partMaxPostNewEdges;
	private static final Logger logger = GraspanLogger.getLogger("ComputedPartProcessor");

	/**
	 * Initializes the heuristic for maximum size of a partition after addition
	 * of new edges
	 */
	public static void initRepartitionConstraints() {
		int numParts = AllPartitions.getPartAllocTab().length;

		// get the total number of edges
		long numEdges = 0;
		long[][] partSizes = SchedulerInfo.getPartSizes();
		for (int i = 0; i < partSizes.length; i++) {
			numEdges = numEdges + partSizes[i][1];
		}

		// average of edges by no. of partitions
		long avgEdgesPerPart = Math.floorDiv(numEdges, numParts);

		// the heuristic for interval max
		long partMax = (long) (avgEdgesPerPart * 0.9);

		// the heuristic for interval max after new edge addition
		// if this threshold is exceeded, the partition is repartitioned
		long heuristic_newPartMax = (long) (partMax + partMax * 0.3);
		// partMaxPostNewEdges = heuristic_newPartMax;
		partMaxPostNewEdges = 60;
	}

	/**
	 * Update data structures based on the computed partitions
	 * 
	 * @param vertices
	 * @param newEdgesLL
	 * @param intervals
	 * @throws IOException
	 */
	public static void processParts(Vertex[] vertices, NewEdgesList[] newEdgesLL, List<LoadedVertexInterval> intervals)
			throws IOException {

		logger.info("Processing partitions after computation.");

		// TEST print
		// for (int i = 0; i < vertices.length; i++) {
		// if (vertices[i].getVertexId() == 36) {
		// System.out.println(vertices[i]);
		// System.out.println(newEdgesLL[i]);
		// }
		// }

		// TODO THESE ARE ON STANDBY (Scheduling)
		long[][] partSizes = SchedulerInfo.getPartSizes();
		long edgeDestCount[][] = SchedulerInfo.getEdgeDestCount();

		// get repartitioning variables
		ArrayList<Integer> splitVertices = RepartitioningData.getSplitVertices();
		TreeSet<Integer> newPartLimits = RepartitioningData.getNewPartLimits();
		HashSet<Integer> repartitionedParts = RepartitioningData.getRepartitionedParts();
		HashSet<Integer> newPartsFrmRepartitioning = RepartitioningData.getNewPartsFrmRepartitioning();
		HashSet<Integer> modifiedParts = RepartitioningData.getModifiedParts();
		HashSet<Integer> unModifiedParts = RepartitioningData.getUnModifiedParts();
		HashSet<Integer> loadedPartsPostProcessing = RepartitioningData.getLoadedPartsPostProcessing();
		HashSet<Integer> partsToSaveByCPP = RepartitioningData.getPartsToSave();
		int[][] loadPartOutDegs = LoadedPartitions.getLoadedPartOutDegs();
		int[] loadedParts = LoadedPartitions.getLoadedParts();

		/*
		 * 1. Scanning each loaded partition, updating degrees data &
		 * modified/unmodified parts, adding repartitioning split points
		 */

		// for each loaded partition
		for (int a = 0; a < loadedParts.length; a++) {

			LoadedVertexInterval part = null;
			// get this partition interval in LVI
			for (int b = 0; b < intervals.size(); b++) {
				if (intervals.get(b).getPartitionId() == loadedParts[a])
					part = intervals.get(b);
			}
			if (part == null) {
				logger.info("Error: LVI does not contain a corresponding interval for any partition in loaded parts.");
			}
			int partId = part.getPartitionId();
			int nodeDestVs[];
			int destPartId;
			int src;
			boolean partHasNewEdges = false;

			// String s = "Processing partition: " + partId + "...\n";
			// s = s + "First Vertex: " + part.getFirstVertex() + "\n";
			// s = s + "Last Vertex: " + part.getLastVertex() + "\n";
			// s = s + "First Vertex Index: " + part.getIndexStart() + "\n";
			// s = s + "Last Vertex Index: " + part.getIndexEnd() + "\n";
			// logger.info(s);

			// get partition's indices in "vertices" data structure
			int partStart = part.getIndexStart();
			int partEnd = part.getIndexEnd();

			// logger.info("LOOOOOOK HEREEEE "+partId+" pstart "+partStart+"
			// partEnd "+partEnd);

			// 1.1. Scan the new edges and update loadPartOutDegs,

			// for each src vertex
			for (int i = partStart; i < partEnd + 1; i++) {
				int numOfNodeVertices = 0;

				// get the actual source id
				// src = i - partStart + part.getFirstVertex();
				src = vertices[i].getVertexId();

				// src extraction test
				// System.out.println("Index of Src in DataStructure" + i);
				// System.out.println("Actual Id of Src" + src);

				// if a new edge for this source exits
				if (newEdgesLL[i] != null) {
					partHasNewEdges = true;

					// for each new edge list node
					for (int j = 0; j < newEdgesLL[i].getSize(); j++) {

						numOfNodeVertices += newEdgesLL[i].getNode(j).getIndex();
						nodeDestVs = newEdgesLL[i].getNode(j).getDstVertices();

						// // 1.1.2. update edgeDestCount for each dest vertex
						// //TODO to ignore for now
						// for (int k = 0; k < numOfNodeVertices; k++) {
						// destPartId =
						// PartitionQuerier.findPartition(nodeDestVs[k]);
						// if (destPartId != -1) {
						// edgeDestCount[partId][destPartId]++;
						// }
						// }
					}

				}
				// if (src == 14) {
				// logger.info("LOOK HERE
				// NOWWWWWW"+vertices[i].getCombinedDeg());
				// }
				// 1.1.1. update degrees data
				loadPartOutDegs[a][PartitionQuerier.getPartArrIdxFrmActualId(src, partId)] = vertices[i]
						.getNumOutEdges() + numOfNodeVertices;
				vertices[i].setCombinedDeg(vertices[i].getNumOutEdges() + numOfNodeVertices);
				// logger.info(
				// "Set Degree of vertex " + vertices[i].getVertexId() + " to "
				// + vertices[i].getCombinedDeg());
			}

			// 1.2. update changed and unchanged parts sets
			if (!partHasNewEdges) {
				unModifiedParts.add(partId);
			} else {
				modifiedParts.add(partId);
			}

			// 1.3. Add repartitioning split vertices

			// keeps track of the size of the current partition
			long partEdgeCount = 0;

			// for each src vertex
			for (int i = partStart; i < partEnd + 1; i++) {

				// get the actual source id
				src = i - partStart + part.getFirstVertex();

				try {
					partEdgeCount += loadPartOutDegs[a][PartitionQuerier.getPartArrIdxFrmActualId(src, partId)];
					// logger.info("partId "+partId);
					// logger.info("loadPartOutDegs[a].length: " +
					// loadPartOutDegs[a].length+"");
					// logger.info("loadPartOutDegs.length: " +
					// loadPartOutDegs.length+"");
					// logger.info("-------------------");
					// logger.info("a "+a);
					// logger.info("actual id
					// "+PartitionQuerier.getPartArrIdxFrmActualId(src,
					// partId));
					// logger.info("src "+src);
				} catch (ArrayIndexOutOfBoundsException e) {
					// logger.info("partId "+partId);
					// logger.info("Error for Partition Id: " + partId);
					// logger.info(loadPartOutDegs[a].length+"");
					// logger.info(loadPartOutDegs.length+"");
					// logger.info("a "+a);
					// logger.info("actual id
					// "+PartitionQuerier.getPartArrIdxFrmActualId(src,
					// partId));
					// logger.info("src "+src);
					// logger.info(
					// "ArrayIndexOutOfBoundsException in: partEdgeCount +=
					// loadPartOutDegs[a][PartitionQuerier.getPartArrIdxFrmActualId(src,
					// partId)];");
				}

				// if the size of the current partition has become
				// larger than the limit, this partition is split, and
				// thus, we add the id of this source vertex as a split
				// point
				if (partEdgeCount > partMaxPostNewEdges & i != partEnd) {
					splitVertices.add(src);
					partEdgeCount = 0;
				}
			}

			// long edgeCount = 0;
			// for (int i = part.getIndexStart(); i < part.getIndexEnd() + 1;
			// i++) {
			// edgeCount += vertices[i].getCombinedDeg();
			// }
			//
			// logger.info("" + part.getPartitionId());
			// logger.info("" + partSizes.length);
			// partSizes[part.getPartitionId()][1] = edgeCount;
		}

		/*
		 * 2. Creating new partitions based on split vertices, and updating all
		 * relevant data structures
		 */

		// testing PartitionQuerier before changing PAT 1/2
		// System.out.println("testing findpartition 13:" +
		// PartitionQuerier.findPartition(13));
		// System.out.println("testing getActualIdFrmPartArrIdx 4th vertex in
		// partition 0:"
		// + PartitionQuerier.getActualIdFrmPartArrIdx(4, 0));
		// System.out.println("testing getLastSrc part 2:" +
		// PartitionQuerier.getLastSrc(2));
		// System.out.println("testing getFirstsSrc part 0:" +
		// PartitionQuerier.getFirstSrc(0));
		// System.out.println("testing getnumunique sources part 2:" +
		// PartitionQuerier.getNumUniqueSrcs(2));
		// System.out.println(
		// "testing getPartArrIdFrmActualId src 38, part 2:" +
		// PartitionQuerier.getPartArrIdxFrmActualId(38, 2));

		// 2.1. Updating partition allocation table.

		int[][] partAllocTable = AllPartitions.getPartAllocTab();

		// 2.1.1. Add splitVertices to newPartLimits
		for (int i = 0; i < splitVertices.size(); i++) {
			newPartLimits.add(splitVertices.get(i));
		}

		// 2.1.2. Add original last vertices of all partitions to newPartLimits
		for (int i = 0; i < partAllocTable.length; i++) {
			newPartLimits.add(partAllocTable[i][1]);
		}

		// 2.1.3. Initialize newPartAllocTable and store the newPartLimits in
		// the new partition allocation table.
		int[][] newPartAllocTable = new int[newPartLimits.size()][2];
		for (int i = 0; i < newPartAllocTable.length; i++) {
			newPartAllocTable[i][0] = -1;
			newPartAllocTable[i][1] = -1;
		}
		int c = 0;
		for (Integer i : newPartLimits) {
			newPartAllocTable[c][1] = i;
			c++;
		}

		// 2.1.4. Get the partition ids from old PAT to new PAT.
		int oldIntervalLast = 0, oldIntervalFirst = 0;
		for (int i = 0; i < newPartAllocTable.length; i++) {
			for (int j = 0; j < partAllocTable.length; j++) {
				if (j == 0) {
					oldIntervalFirst = 1;
				} else {
					oldIntervalFirst = partAllocTable[j - 1][1] + 1;
				}
				oldIntervalLast = partAllocTable[j][1];
				if (newPartAllocTable[i][1] >= oldIntervalFirst & newPartAllocTable[i][1] <= oldIntervalLast) {
					newPartAllocTable[i][0] = partAllocTable[j][0];
					partAllocTable[j][0] = -1;
				}
			}
		}

		// 2.1.5. Generate new partition ids.
		int newPartId = partAllocTable.length;
		for (int i = 0; i < newPartAllocTable.length; i++) {
			if (newPartAllocTable[i][0] == -1) {
				newPartAllocTable[i][0] = newPartId;

				// add id of new partition to newPartsFrmRepartitioning set
				newPartsFrmRepartitioning.add(newPartId);

				newPartId++;
			}
		}

		AllPartitions.setPartAllocTab(newPartAllocTable);
		GlobalParams.setNumParts(newPartAllocTable.length);

		// testing PartitionQuerier after changing PAT 2/2
		// System.out.println("testing findPartition 7:" +
		// PartitionQuerier.findPartition(7));
		// System.out.println("testing getActualIdFrmPartArrIdx 2nd vertex in
		// partition 5:"
		// + PartitionQuerier.getActualIdFrmPartArrIdx(2, 5));
		// System.out.println("testing getLastSrc part 4:" +
		// PartitionQuerier.getLastSrc(4));
		// System.out.println("testing getFirstsSrc part 6:" +
		// PartitionQuerier.getFirstSrc(6));
		// System.out.println("testing getNumUnique sources part 3:" +
		// PartitionQuerier.getNumUniqueSrcs(3));
		// System.out.println(
		// "testing getPartArrIdFrmActualId src 8, part 4:" +
		// PartitionQuerier.getPartArrIdxFrmActualId(8, 4));

		// print new partitions /test code
		// for (int i = 0; i < newPartAllocTable.length; i++) {
		// System.out.println(newPartAllocTable[i][0] + " " +
		// newPartAllocTable[i][1]);
		// }

		// 2.2. Updating repartitioned/modifiedParts and updating loadedParts
		// (partitions loaded prior to this computation) for RELOAD_PLAN_2.
		// loadedParts will be needed for next loading using this reload
		// plan.)
		for (int i = 0; i < splitVertices.size(); i++) {
			for (int j = 0; j < loadedParts.length; j++) {
				if (loadedParts[j] == PartitionQuerier.findPartition(splitVertices.get(i))) {

					// 2.2.1. add id of repartitioned partition to
					// repartitionedParts set
					repartitionedParts.add(loadedParts[j]);
					modifiedParts.remove(loadedParts[j]);

					// 2.2.2. once a partition is repartitioned, we don't
					// consider it loaded, thus we set it to MIN_VALUE
					if (GlobalParams.getReloadPlan().compareTo("RELOAD_PLAN_2") == 0)
						loadedParts[j] = Integer.MIN_VALUE;

					break;
				}
			}
		}

		// Post processing - Loaded parts test
		// System.out.println("Loaded parts after processing computed parts");
		// for (int j = 0; j < loadedParts.length; j++) {
		// System.out.println(loadedParts[j]);
		// }
		//

		// 2.3. Update LoadedVertexIntervals

		// 2.3.1. Collect Ids of all loaded partitions in
		// loadedPartsPostProcessing
		for (Integer partId : repartitionedParts)
			loadedPartsPostProcessing.add(partId);
		for (Integer partId : newPartsFrmRepartitioning)
			loadedPartsPostProcessing.add(partId);
		for (Integer partId : modifiedParts)
			loadedPartsPostProcessing.add(partId);
		for (Integer partId : unModifiedParts)
			loadedPartsPostProcessing.add(partId);

		// loadedPartsPostProcessing test
		// System.out.println(loadedPartsPostProcessing);

		// 2.3.2. Scan vertices data structure and store partition interval
		// indices in LoadedVertexIntervals
		int src = 0, indexSt = 0, indexEd = 0, minSrcTest = 0;
		boolean intervalFound;
		for (int i = 0; i < vertices.length; i++) {
			if (vertices[i] != null) {
				minSrcTest = 0;
				src = vertices[i].getVertexId();
				// logger.info("Scanning src " + src + " idx " + i);
				for (Integer loadedPartId : loadedPartsPostProcessing) {
					if (src == PartitionQuerier.getFirstSrc(loadedPartId)) {

						// For an existing interval
						// find the interval
						intervalFound = false;
						for (LoadedVertexInterval interval : intervals) {
							if (interval.getPartitionId() == loadedPartId) {
								intervalFound = true;
								interval.setLastVertex(PartitionQuerier.getLastSrc(loadedPartId));
								indexSt = i;
								interval.setIndexStart(indexSt);
								indexEd = indexSt + PartitionQuerier.getNumUniqueSrcs(loadedPartId) - 1;
								interval.setIndexEnd(indexEd);
								break;
							}
						}

						if (!intervalFound) {
							// we have a new interval to add
							LoadedVertexInterval interval = new LoadedVertexInterval(src,
									PartitionQuerier.getLastSrc(loadedPartId), loadedPartId);
							indexSt = i;
							interval.setIndexStart(indexSt);
							indexEd = indexSt + PartitionQuerier.getNumUniqueSrcs(loadedPartId) - 1;
							interval.setIndexEnd(indexEd);
							intervals.add(interval);
						}

						// logger.info("Interval found from source " + src + ".
						// part: " + loadedPartId + ", intervalSt(i): "
						// + indexSt + ", intervalEd: " + indexEd);

						// moving i forward ensures we scan only the minimum
						// vertices
						i = indexEd;

						minSrcTest = 1;
						break;
					}
				}
				if (minSrcTest == 0)
					logger.info("ERROR: Reading a source that is not a minimum for any partition.");

			}
		}

		// 2.4. updating basic scheduler (Part Sizes)
		int[][] pat = AllPartitions.getPartAllocTab();
		long[][] newPartSizes = new long[pat.length][2];

		// 2.4.1. get ids in newPartsizes
		for (int i = 0; i < pat.length; i++) {
			newPartSizes[i][0] = pat[i][0];
		}

		// 2.4.2. get current partSizes values in newPartSizes
		for (int i = 0; i < partSizes.length; i++) {
			for (int j = 0; j < newPartSizes.length; j++) {
				if (newPartSizes[j][0] == partSizes[i][0]) {
					newPartSizes[j][1] = partSizes[i][1];
				}
			}
		}

		// 2.4.3. get the sizes from by calling getCombinedDeg from each
		// interval
		long edgeCount = 0;
		for (LoadedVertexInterval interval : intervals) {
			for (int i = interval.getIndexStart(); i < interval.getIndexEnd() + 1; i++) {
				edgeCount += vertices[i].getCombinedDeg();
			}
			for (int j = 0; j < newPartSizes.length; j++) {
				if (newPartSizes[j][0] == interval.getPartitionId()) {
					newPartSizes[j][1] = edgeCount;
				}
			}
			logger.info(interval.getPartitionId() + "");
		}

		SchedulerInfo.setPartSizes(newPartSizes);

		// for (int i = 0; i < pat.length; i++) {
		// logger.info("hehre" + pat[i][0]);
		// }

		// 2.5. Create partsToSave set.

		// Add repartitionedParts and newPartsFrmRepartitioning to
		// partsToSave set if using RELOAD_PLAN_2.
		if (GlobalParams.getReloadPlan().compareTo("RELOAD_PLAN_2") == 0)

		{
			for (Integer Id : repartitionedParts)
				partsToSaveByCPP.add(Id);
			for (Integer Id : newPartsFrmRepartitioning)
				partsToSaveByCPP.add(Id);

			logger.info("Partitions that have been repartitioned:");
			for (Integer Id : repartitionedParts)
				logger.info(Id + "");

			logger.info("Partitions that have been newly created as a result of repartitioning:");
			for (Integer Id : newPartsFrmRepartitioning)
				logger.info(Id + "");

		}

		// Post processing - Parts to save test
		// String s = "Printing parts to save\n";
		// s = s + "Reload Plan : " + GlobalParams.getReloadPlan() +
		// "\n";
		// s = s + partsToSave;
		// logger.info(s);

		/*
		 * 3. Save partitions to disk.
		 */

		logger.info("The partitions to save as determined by Computed-part-processor:");
		for (

		Integer partitionId : partsToSaveByCPP)
			logger.info("" + partitionId);

		// 3.1. save repartitioned partition and newly generated partitions
		// iterate over saveParts and get partitionId
		for (Integer partitionId : partsToSaveByCPP)

			storePart(vertices, newEdgesLL, intervals, partitionId);

		// 3.2. save degree of those partitions.
		// iterate over saveParts and get partitionId
		for (Integer partitionId : partsToSaveByCPP)
			storePartDegs(vertices, intervals, partitionId);

		// loaded intervals test before saving partitions 1/2
		// String s = "Loaded intervals before saving:\n";
		// for (int i = 0; i < intervals.size(); i++)
		// s = s + intervals.get(i).getPartitionId() + " ";
		// logger.info(s);

		// 3.3. Remove saved partitions from LoadedVertexIntervals
		for (int i = 0; i < intervals.size(); i++) {
			if (partsToSaveByCPP.contains(intervals.get(i).getPartitionId())) {
				intervals.remove(i);
				// reset i
				i--;
			}
		}

		partsToSaveByCPP.clear();

		// loaded intervals test after saving partitions 2/2
		// String s1 = "Loaded intervals after saving:\n";
		// for (int i = 0; i < intervals.size(); i++)
		// s1 = s1 + intervals.get(i).getPartitionId() + " ";
		// logger.info(s1);

		RepartitioningData.clearRepartitioningVars();
	}

	/**
	 * Stores a partition to disk.
	 * 
	 * @param vertices
	 * @param newEdgesLL
	 * @param intervals
	 * @param partitionId
	 * @throws IOException
	 */
	private static void storePart(Vertex[] vertices, NewEdgesList[] newEdgesLL, List<LoadedVertexInterval> intervals,
			Integer partitionId) throws IOException {

		logger.info("Updating " + GlobalParams.baseFilename + ".partition." + partitionId);

		// clear current file
		DataOutputStream partOutStrm = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(GlobalParams.baseFilename + ".partition." + partitionId, false)));
		partOutStrm.close();

		partOutStrm = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(GlobalParams.baseFilename + ".partition." + partitionId, true)));

		int srcVId, destVId, count;
		int edgeValue;
		for (int i = 0; i < intervals.size(); i++) {

			// locate the required interval in "vertices"
			if (partitionId == intervals.get(i).getPartitionId()) {

				// scan each vertex in this interval in "vertices" datastructure
				for (int j = intervals.get(i).getIndexStart(); j < intervals.get(i).getIndexEnd() + 1; j++) {

					count = vertices[j].getCombinedDeg();
					if (count == 0) {
						continue;
					}
					// write the srcId
					srcVId = vertices[j].getVertexId();
					partOutStrm.writeInt(srcVId);

					// write the count
					partOutStrm.writeInt(count);

					// scan each edge (original edge) in list of each vertex in
					// this interval
					for (int k = 0; k < vertices[j].getNumOutEdges(); k++) {

						// write the destId-edgeValue pair
						destVId = vertices[j].getOutEdge(k);
						edgeValue = vertices[j].getOutEdgeValue(k);
						partOutStrm.writeInt(destVId);
						partOutStrm.writeByte(edgeValue);

					}

					// scan each newEdge in list of each vertex in
					// this interval

					if (newEdgesLL[j] != null) {

						// for each new edge list node
						for (int k = 0; k < newEdgesLL[j].getSize(); k++) {

							// for each edge in the new edge list node
							for (int l = 0; l < newEdgesLL[j].getNode(k).getIndex(); l++) {

								// write the new destId-edgeValue pair
								destVId = newEdgesLL[j].getNode(k).getNewOutEdge(l);
								edgeValue = newEdgesLL[j].getNode(k).getNewOutEdgeValue(l);
								partOutStrm.writeInt(destVId);
								partOutStrm.writeByte(edgeValue);

							}
						}

					}
				}
			}
		}

		partOutStrm.close();

	}

	/**
	 * Stores degrees of a partition.
	 * 
	 * @param vertices
	 * @param intervals
	 * @param partitionId
	 * @throws IOException
	 */
	public static void storePartDegs(Vertex[] vertices, List<LoadedVertexInterval> intervals, Integer partitionId)
			throws IOException {

		logger.info("Updating " + GlobalParams.baseFilename + ".partition." + partitionId + ".degrees");

		PrintWriter partDegOutStrm = new PrintWriter(new BufferedWriter(
				new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId + ".degrees", false)));
		partDegOutStrm.close();

		partDegOutStrm = new PrintWriter(new BufferedWriter(
				new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId + ".degrees", true)));

		int srcVId, deg;

		for (int i = 0; i < intervals.size(); i++) {

			// locate the required interval in "vertices"
			if (partitionId == intervals.get(i).getPartitionId()) {

				// scan each vertex in this interval in "vertices" datastructure
				for (int j = intervals.get(i).getIndexStart(); j < intervals.get(i).getIndexEnd() + 1; j++) {

					// get srcId and deg
					srcVId = vertices[j].getVertexId();
					deg = vertices[j].getCombinedDeg();
					if (deg == 0)
						continue;
					partDegOutStrm.println(srcVId + "\t" + deg);

				}
			}
		}

		partDegOutStrm.close();

	}

}
