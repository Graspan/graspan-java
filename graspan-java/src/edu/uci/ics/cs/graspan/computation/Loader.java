package edu.uci.ics.cs.graspan.computation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.GlobalParams;
import edu.uci.ics.cs.graspan.datastructures.LoadedPartitions;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.NewEdgesList;
import edu.uci.ics.cs.graspan.datastructures.PartitionQuerier;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.scheduler.SchedulerInfo;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

/**
 * This program loads partitions into the memory.
 * 
 * @author Aftab
 *
 */
public class Loader {

	private static final Logger logger = GraspanLogger.getLogger("graphdtc partitionloader");

	private static Vertex[] vertices = null;
	private static NewEdgesList[] newEdgeLists = null;

	private List<LoadedVertexInterval> intervals = new ArrayList<LoadedVertexInterval>();

	private String baseFilename = "";
	private String reloadPlan = "";
	private String preservePlan = "";

	private int numParts = 0;

	/**
	 * Initializes the partition loader, reads in the partition allocation
	 * table, and reads in the edgedestcountInfo (Should be called only once.)
	 * 
	 * @param baseFilename
	 * @param numParts
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public Loader() throws NumberFormatException, IOException {

		this.baseFilename = GlobalParams.getBasefilename();
		this.numParts = GlobalParams.getNumParts();
		this.reloadPlan = GlobalParams.getReloadPlan();
		this.preservePlan = GlobalParams.getPreservePlan();

		// get the partition allocation table
		readPartAllocTable();

		// get the scheduling info
		readSchedulingInfo();

		// initialize variables for partition loading based on number of
		// partitions to load.
		preliminaryInit();

		// scheduler info test
		// SchedulerInfo.printData();
	}

	/**
	 * Initialize variables for partition loading only based on number of
	 * partitions to load.
	 */
	private void preliminaryInit() {

		int newPartsToLoad[] = new int[GlobalParams.getNumPartsPerComputation()];
		int loadedParts[] = new int[GlobalParams.getNumPartsPerComputation()];
		LinkedHashSet<Integer> partsToSave = new LinkedHashSet<Integer>();

		for (int i = 0; i < loadedParts.length; i++) {
			newPartsToLoad[i] = Integer.MIN_VALUE;
			loadedParts[i] = Integer.MIN_VALUE;
		}

		int loadedPartOutDegs[][] = new int[GlobalParams.getNumPartsPerComputation()][];
		int loadedPartEdges[][][] = new int[GlobalParams.getNumPartsPerComputation()][][];
		byte loadedPartEdgeVals[][][] = new byte[GlobalParams.getNumPartsPerComputation()][][];

		LoadedPartitions.setPartsToSave(partsToSave);
		LoadedPartitions.setNewParts(newPartsToLoad);
		LoadedPartitions.setLoadedParts(loadedParts);
		LoadedPartitions.setLoadedPartOutDegs(loadedPartOutDegs);
		LoadedPartitions.setLoadedPartEdges(loadedPartEdges);
		LoadedPartitions.setLoadedPartEdgeVals(loadedPartEdgeVals);

		if (preservePlan.compareTo("RESTORE_PLAN_1") == 0) {
			vertices = new Vertex[200];
			newEdgeLists = new NewEdgesList[200];
		}
	}

	/**
	 * Loads partitions in the memory
	 * 
	 * @param partsToLoad
	 * @throws IOException
	 */
	public void loadParts(int[] partsToLoad) throws IOException {

		String str = "";
		for (int i = 0; i < partsToLoad.length; i++) {
			str = str + partsToLoad[i] + " ";
		}
		logger.info("Loading partitions : " + str + "...");

		// update newPartsToLoad
		updateNewPartsAndLoadedParts(partsToLoad);

		// update loadedPartOutDegrees
		updateDegsOfPartsToLoad();

		// initialize data structures of the partitions to load
		initVarsOfPartsToLoad();

		logger.info("Initialized data structures for partitions to load.");
		// fill the partition data structures
		fillVarsOfPartsToLoad();

		int loadedPartOutDegs[][] = LoadedPartitions.getLoadedPartOutDegs();
		int partEdges[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeVals[][][] = LoadedPartitions.getLoadedPartEdgeVals();
		int newParts[] = LoadedPartitions.getNewParts();

		// sorting the partitions
		for (int i = 0; i < newParts.length; i++) {
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(newParts[i]); j++) {
				int low = 0;
				int high = loadedPartOutDegs[i][j] - 1;
				Utilities.quickSort(partEdges[i][j], partEdgeVals[i][j], low, high);
			}
		}
		logger.info("Sorted loaded partitions.");

		// reset newParts
		for (int i = 0; i < newParts.length; i++) {
			newParts[i] = Integer.MIN_VALUE;
		}

		// loaded partitions test
		// LoadedPartitions.printLoadedPartitions();

		// loaded parts degrees test
		// LoadedPartitions.printLoadedPartOutDegs();
		// System.exit(0);
	}

	/**
	 * Gets the partition allocation table. (Should be called only once during
	 * first load)
	 * 
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void readPartAllocTable() throws NumberFormatException, IOException {

		// initialize partAllocTable variable
		int partAllocTable[][] = new int[numParts][2];

		/*
		 * Scan the partition allocation table file
		 */
		BufferedReader inPartAllocTabStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".partAllocTable"))));
		String ln, tok[];

		int i = 0;
		while ((ln = inPartAllocTabStrm.readLine()) != null) {
			tok = ln.split("\t");
			// store partition allocation table in memory
			partAllocTable[i][0] = Integer.parseInt(tok[0]);
			partAllocTable[i][1] = Integer.parseInt(tok[1]);
			i++;
		}
		AllPartitions.setPartAllocTab(partAllocTable);
		inPartAllocTabStrm.close();

		logger.info("Loaded " + baseFilename + ".partAllocTable");

	}

	/**
	 * Gets the Scheduling Info. (Should be called only once during first load)
	 * 
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	private void readSchedulingInfo() throws NumberFormatException, IOException {

		// initialize edgeDestCount and partSizes variables
		long edgeDestCount[][] = new long[numParts][numParts];
		long partSizes[] = new long[numParts];

		/*
		 * Scan the edge destination counts file
		 */
		BufferedReader inEdgeDestCountStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".edgeDestCounts"))));
		String ln;

		int partA, partB;
		String[] tok;
		while ((ln = inEdgeDestCountStrm.readLine()) != null) {
			tok = ln.split("\t");
			partA = Integer.parseInt(tok[0]);
			partB = Integer.parseInt(tok[1]);

			// store edge destination counts in memory
			edgeDestCount[partA][partB] = Long.parseLong(tok[2]);
		}
		SchedulerInfo.setEdgeDestCount(edgeDestCount);

		inEdgeDestCountStrm.close();

		logger.info("Loaded " + baseFilename + ".edgeDestCounts");

		/*
		 * Scan the partSizes file
		 */
		BufferedReader inPartSizesStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".partSizes"))));

		int j = 0;
		while ((ln = inPartSizesStrm.readLine()) != null) {
			// store partSizes in memory
			partSizes[j++] = Long.parseLong(ln);
		}
		SchedulerInfo.setPartSizes(partSizes);

		inPartSizesStrm.close();
		logger.info("Loaded partition sizes file " + baseFilename + ".partSizes");

	}

	public Vertex[] getVertices() {
		return vertices;
	}

	public List<LoadedVertexInterval> getIntervals() {
		return intervals;
	}

	public NewEdgesList[] getNewEdgeLists() {
		return newEdgeLists;
	}

	/**
	 * Computes the next set of parts that are to be loaded in the memory.
	 * 
	 * @param partsToLoad
	 * @throws IOException
	 */
	private void updateNewPartsAndLoadedParts(int partsToLoad[]) throws IOException {
		/*
		 * NOTE: At no point will partsToLoad be equal to loadedparts.
		 */

		// TODO INCOMPLETE - SINCE RELOAD PLAN 1 IS THE WORST PLAN, WE SHALL
		// IGNORE THIS
		if (this.reloadPlan.compareTo("RELOAD_PLAN_1") == 0) {
			int[] newParts = LoadedPartitions.getNewParts();
			newParts = partsToLoad;
			LoadedPartitions.setNewParts(newParts);
		}

		if (this.reloadPlan.compareTo("RELOAD_PLAN_2") == 0) {
			int[] loadedParts = LoadedPartitions.getLoadedParts();
			int[] newParts = LoadedPartitions.getNewParts();
			HashSet<Integer> partsToSave = LoadedPartitions.getPartsToSave();
			HashSet<Integer> tempSet = new HashSet<Integer>();

			/*
			 * partid loading test 1/2 (comment loadedParts, newParts
			 * initialization above, change name of parameter partsToLoad above)
			 * 
			 */
			// System.out.println("START");
			//
			// int[] loadedParts = { 8, 2, 3, Integer.MIN_VALUE, 6, 7 };
			// System.out.println("loadedParts");
			// for (int i = 0; i < loadedParts.length; i++)
			// System.out.print(loadedParts[i] + " ");
			// System.out.println();
			//
			// int[] partsToLoad = { 1, 3, 8, 7, 10, 11 };
			// System.out.println("partsToLoad");
			// for (int i = 0; i < partsToLoad.length; i++)
			// System.out.print(partsToLoad[i] + " ");
			// System.out.println();
			//
			// int[] newParts = { Integer.MIN_VALUE, Integer.MIN_VALUE,
			// Integer.MIN_VALUE, Integer.MIN_VALUE,
			// Integer.MIN_VALUE, Integer.MIN_VALUE };
			//
			// System.out.println("newParts");
			// for (int i = 0; i < newParts.length; i++)
			// System.out.print(newParts[i] + " ");
			// System.out.println();

			// 1. Get parts that are not part of the next computation and should
			// be saved.

			// 1.1. Get ids of all parts for next computation.
			for (int i = 0; i < partsToLoad.length; i++) {
				tempSet.add(partsToLoad[i]);
			}
			// 1.2. Add the ones not included for next computation to
			// partsToSave
			for (int i = 0; i < loadedParts.length; i++) {
				if (!tempSet.contains(loadedParts[i]) & loadedParts[i] != Integer.MIN_VALUE) {
					partsToSave.add(loadedParts[i]);
				}
			}

			// test partsToSave
			System.out.println("Partitions to save:");
			System.out.println(partsToSave);

			// 2. Save PartsSet

			// 2.1. save repartitioned partition and newly generated partitions
			// iterate over saveParts and get partitionId
			for (Integer partitionId : partsToSave)
				storePart(getVertices(), getNewEdgeLists(), getIntervals(), partitionId);

			// 2.2. save degree of those partitions.
			// iterate over saveParts and get partitionId
			for (Integer partitionId : partsToSave)
				storePartDegs(getVertices(), getIntervals(), partitionId);

			// 2.3. Remove saved partitions from LoadedVertexIntervals
			for (int i = 0; i < getIntervals().size(); i++) {
				if (partsToSave.contains(getIntervals().get(i).getPartitionId())) {
					getIntervals().remove(i);
					// reset i
					i--;
				}
			}

			partsToSave.clear();

			tempSet.clear();

			// 3. Update newParts and loadedParts.

			// 3.1. Get ids of all parts currently loaded
			for (int i = 0; i < loadedParts.length; i++) {
				tempSet.add(loadedParts[i]);
			}

			// 3.2. Get ids of partitions not loaded and store them in the
			// positions of partitions that are to be saved
			for (int i = 0; i < partsToLoad.length; i++) {
				// if the partition is not already loaded
				if (!tempSet.contains(partsToLoad[i])) {
					// find the partition that is loaded but no longer required
					// (i.e. in savePartsSet)
					for (int j = 0; j < loadedParts.length; j++) {
						if (loadedParts[j] == Integer.MIN_VALUE) {
							// store the new id in loadedParts in place of the
							// partition to save
							loadedParts[j] = partsToLoad[i];

							// store the new id in the corresponding location in
							// newParts
							newParts[j] = partsToLoad[i];

							break;
						}
						if (partsToSave.contains(loadedParts[j])) {

							// store the new id in loadedParts in place of the
							// partition to save
							loadedParts[j] = partsToLoad[i];

							// store the new id in the corresponding location in
							// newParts
							newParts[j] = partsToLoad[i];

							// remove this partition from savePartsSet
							partsToSave.remove(loadedParts[j]);
							break;
						}
					}
				}
			}

			/*
			 * partid loading test 2/2
			 */
			// System.out.println("AFTER STORING NEW PARTS");
			// System.out.println("newParts");
			// for (int i = 0; i < newParts.length; i++)
			// System.out.print(newParts[i] + " ");
			// System.out.println();
			// System.out.println("loadedParts");
			// for (int i = 0; i < loadedParts.length; i++)
			// System.out.print(loadedParts[i] + " ");
			// System.out.println();
			// System.out.println("partsToLoad");
			// for (int i = 0; i < partsToLoad.length; i++)
			// System.out.print(partsToLoad[i] + " ");
			// System.out.println();
			// System.exit(0);

		}
	}

	/**
	 * Updates the degrees of the source vertices of the partitions that are to
	 * be loaded.
	 * 
	 * @param baseFilename
	 * @param partsToLoad
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	private void updateDegsOfPartsToLoad() throws IOException {

		int[] newParts = LoadedPartitions.getNewParts();
		int[][] partOutDegs = LoadedPartitions.getLoadedPartOutDegs();

		/*
		 * Initialize the degrees array for each new partition to load. We shall
		 * not reinitialize the degrees of the partitions that have already been
		 * loaded (This does not apply for RELOAD_PLAN_1).
		 */

		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != Integer.MIN_VALUE) {
				// initialize Dimension 2 (Total no. of Unique SrcVs for a
				// Partition)
				partOutDegs[i] = new int[PartitionQuerier.getNumUniqueSrcs(newParts[i])];
				// remember to use this only for loading partitions that aren't
				// currently loaded.
			}
		}

		/*
		 * Scan degrees file of each partition
		 */
		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != Integer.MIN_VALUE) {
				BufferedReader outDegInStrm = new BufferedReader(new InputStreamReader(
						new FileInputStream(new File(baseFilename + ".partition." + newParts[i] + ".degrees"))));

				String ln;
				while ((ln = outDegInStrm.readLine()) != null) {

					String[] tok = ln.split("\t");

					// get the srcVId and degree
					int srcVId = Integer.parseInt(tok[0]);
					int deg = Integer.parseInt(tok[1]);

					partOutDegs[i][srcVId - PartitionQuerier.getFirstSrc(newParts[i])] = deg;
					// this will be later updated in processParts() of
					// ComputedPartProcessor if new edges are added for this
					// source vertex during computation.
				}
				outDegInStrm.close();

				logger.info("Loaded " + baseFilename + ".partition." + newParts[i] + ".degrees");
			}
		}
	}

	/**
	 * Gets the degrees of the source vertices of the partitions that are to be
	 * loaded. (Deprecated- this method reads the degrees file of the entire
	 * graph.)
	 * 
	 * @param baseFilename
	 * @param partsToLoad
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	@SuppressWarnings("unused")
	private void getDegrees(String baseFilename, int[] partsToLoad) throws NumberFormatException, IOException {

		/*
		 * Initialize the degrees array for each partition
		 */

		// initialize Dimension 1 (Total no. of Partitions)
		int[][] partOutDegs = new int[partsToLoad.length][];

		for (int i = 0; i < partsToLoad.length; i++) {

			// initialize Dimension 2 (Total no. of Unique SrcVs for a
			// Partition)
			partOutDegs[i] = new int[PartitionQuerier.getNumUniqueSrcs(partsToLoad[i])];
		}

		/*
		 * Scan the degrees file
		 */
		BufferedReader outDegInStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".degrees"))));

		System.out.print("Reading degrees file (" + baseFilename
				+ ".degrees) to obtain degrees of source vertices in partitions to load");

		String ln;
		while ((ln = outDegInStrm.readLine()) != null) {

			String[] tok = ln.split("\t");

			// get the srcVId and degree
			int srcVId = Integer.parseInt(tok[0]);
			int deg = Integer.parseInt(tok[1]);

			for (int i = 0; i < partsToLoad.length; i++) {

				// check if the srcVId belongs to this partition
				if (!PartitionQuerier.inPartition(srcVId, partsToLoad[i])) {
					continue;
				} else {
					try {
						partOutDegs[i][srcVId - PartitionQuerier.getFirstSrc(partsToLoad[i])] = deg;
					} catch (ArrayIndexOutOfBoundsException e) {
					}
				}
			}
		}
		LoadedPartitions.setLoadedPartOutDegs(partOutDegs);
		outDegInStrm.close();
		System.out.println("Done");
	}

	/**
	 * Initializes data structures of the partitions to load
	 */
	private void initVarsOfPartsToLoad() {

		int[][] partOutDegs = LoadedPartitions.getLoadedPartOutDegs();
		int[] loadedParts = LoadedPartitions.getLoadedParts();
		int[] newParts = LoadedPartitions.getNewParts();

		if (GlobalParams.getPreservePlan().compareTo("PRESERVE_PLAN_2") == 0) {
			// initializing new data structures
			int totalNumVertices = 0;
			for (int i = 0; i < loadedParts.length; i++) {
				totalNumVertices += PartitionQuerier.getNumUniqueSrcs(loadedParts[i]);
			}
			vertices = new Vertex[totalNumVertices];
			newEdgeLists = new NewEdgesList[totalNumVertices];
		}

		// System.out.println("Loaded Vertex Intervals size");
		// System.out.println(intervals.size());
		// System.out.println(vertices.length);

		TreeSet<Integer> originalLoadedPartIndices = new TreeSet<Integer>();
		TreeSet<Integer> newLoadedPartIndices = new TreeSet<Integer>();
		// using arraylist as we need to maintain duplicates and order
		ArrayList<Integer> indexDiffs = new ArrayList<Integer>();

		// edge-data preservation test 1/ setting dummy vertices and
		// newedgelists (comment
		// originalLoadedPartIndices for-loop below when using this.)
		// int[] my_vertices = { 5, 12, 13, 10, 1, 2, 3, 3, 4, 8, 7, 6, 7, 3, 2,
		// 10, 11, 9, 8 };
		// int[] my_newEdges = { 5, 12, 13, 10, 1, 2, 3, 3, 4, 8, 7, 6, 7, 3, 2,
		// 10, 11, 9, 8 };
		// int my_temp_newEdges = 0;
		// int[] my_indexes = { 3, 7, 10, 12, 14, 18 };
		// for (int i = 0; i < my_indexes.length; i++)
		// originalLoadedPartIndices.add(my_indexes[i]);
		// System.out.println("originalLoadedPartIndices:");
		// System.out.println(originalLoadedPartIndices);

		for (LoadedVertexInterval interval : intervals) {
			originalLoadedPartIndices.add(interval.getIndexStart());
			originalLoadedPartIndices.add(interval.getIndexEnd());
		}

		Iterator<Integer> itr = originalLoadedPartIndices.iterator();
		int firstIdx = 0, lastIdx = 0;
		while (itr.hasNext()) {
			firstIdx = itr.next();
			lastIdx = itr.next();
			indexDiffs.add(lastIdx - firstIdx);
		}

		// edge-data preservation test 2/ IndexDiffs
		// System.out.println("indexDiffs:");
		// System.out.println(indexDiffs);

		int newIndexSt = 0;
		for (Integer diff : indexDiffs) {
			newLoadedPartIndices.add(newIndexSt);
			newLoadedPartIndices.add(newIndexSt + diff);
			newIndexSt = newIndexSt + diff + 1;
		}

		// edge-data preservation test 3/ newLoadedPartIndices
		// System.out.println("newLoadedPartIndices:");
		// System.out.println(newLoadedPartIndices);

		NewEdgesList temp_newEdgeList = null;
		int oldIdxMin, oldIdxMax, newIdxMin, newIdxMax = 0;
		Iterator<Integer> itrOld = originalLoadedPartIndices.iterator();
		Iterator<Integer> itrNew = newLoadedPartIndices.iterator();
		while (itrOld.hasNext()) {
			newIdxMin = itrNew.next();
			newIdxMax = itrNew.next();
			oldIdxMin = itrOld.next();
			oldIdxMax = itrOld.next();// just to move the index forward
			for (int i = newIdxMin; i <= newIdxMax; i++) {

				// preserve vertices
				vertices[i] = vertices[oldIdxMin];

				// edge-data preservation test 4/ shifting vertices
				// my_vertices[i] = my_vertices[oldIdxMin];

				// preserve new edges
				temp_newEdgeList = newEdgeLists[oldIdxMin];
				newEdgeLists[oldIdxMin] = newEdgeLists[i];
				newEdgeLists[i] = temp_newEdgeList;

				// edge-data preservation test 5/ swapping newEdges
				// my_temp_newEdges = my_newEdges[oldIdxMin];
				// my_newEdges[oldIdxMin] = my_newEdges[i];
				// my_newEdges[i] = my_temp_newEdges;

				oldIdxMin++;
			}
		}

		// resetting vertices not in loaded partitions (unnecessary)
		// for (int i = newIdxMax + 1; i < vertices.length; i++) {
		// vertices[i] = null;
		// }

		// edge-data preservation test 6/ (use if above is used)
		// for (int i = newIdxMax + 1; i < my_vertices.length; i++) {
		// my_vertices[i] = -1;
		// }

		// edge-data preservation test 7/ final vertices and edges
		// System.out.println("final my_vertices");
		// System.out.print("[ ");
		// for (int i = 0; i < my_vertices.length; i++) {
		// System.out.print(my_vertices[i] + " ");
		// }
		// System.out.println(" ]");
		//
		// System.out.println("final my_newEdges");
		// System.out.print("[ ");
		// for (int i = 0; i < my_newEdges.length; i++) {
		// System.out.print(my_newEdges[i] + " ");
		// }
		// System.out.println(" ]");
		// System.exit(0);

		int partEdges[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeVals[][][] = LoadedPartitions.getLoadedPartEdgeVals();

		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != Integer.MIN_VALUE) {

				// initialize Dimension 2 (Total no. of Unique SrcVs for a
				// Partition)
				partEdges[i] = new int[PartitionQuerier.getNumUniqueSrcs(newParts[i])][];
				partEdgeVals[i] = new byte[PartitionQuerier.getNumUniqueSrcs(newParts[i])][];

				for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(newParts[i]); j++) {

					// initialize Dimension 3 (Total no. of Out-edges for a
					// SrcV)
					partEdges[i][j] = new int[partOutDegs[i][j]];
					partEdgeVals[i][j] = new byte[partOutDegs[i][j]];

				}
			}
		}

		// set vertices data structure
		int vertexIdx = 0;
		for (int i = 0; i < loadedParts.length; i++) {
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(loadedParts[i]); j++) {
				int vertexId = PartitionQuerier.getActualIdFrmPartArrIdx(j, loadedParts[i]);
				vertices[vertexIdx] = new Vertex(vertexIdx, vertexId, partEdges[i][j], partEdgeVals[i][j]);
				vertexIdx++;
			}
		}

	}

	/**
	 * Reads the partition files and stores them in arrays
	 * 
	 * @param partInputStream
	 * @throws IOException
	 */
	private void fillVarsOfPartsToLoad() throws IOException {

		int[] newParts = LoadedPartitions.getNewParts();
		int[] loadedparts = LoadedPartitions.getLoadedParts();
		int[][][] partEdges = LoadedPartitions.getLoadedPartEdges();
		byte[][][] partEdgeVals = LoadedPartitions.getLoadedPartEdgeVals();

		int indexSt = 0;
		int indexEd = 0;

		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != Integer.MIN_VALUE) {

				DataInputStream partInStrm = new DataInputStream(
						new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + newParts[i])));

				// stores the position of last filled edge (destV) and the edge
				// val
				// in partEdges and partEdgeVals for a source vertex
				// for a partition
				int[] lastAddedEdgePos = new int[PartitionQuerier.getNumUniqueSrcs(newParts[i])];
				for (int j = 0; j < lastAddedEdgePos.length; j++) {
					lastAddedEdgePos[j] = -1;
				}

				while (partInStrm.available() != 0) {
					{
						try {
							// get srcVId
							int src = partInStrm.readInt();

							// get corresponding arraySrcVId of srcVId
							int arraySrcVId = src - PartitionQuerier.getFirstSrc(newParts[i]);

							// get count (number of destVs from srcV in the
							// current
							// list
							// of the partition file)
							int count = partInStrm.readInt();

							// get dstVId & edgeVal and store them in the
							// corresponding
							// arrays
							for (int j = 0; j < count; j++) {

								// dstVId
								partEdges[i][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partInStrm.readInt();

								// edgeVal
								partEdgeVals[i][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partInStrm.readByte();

								// increment the last added position for this
								// row
								lastAddedEdgePos[arraySrcVId]++;
							}

						} catch (Exception exception) {
							break;
						}
					}
				}

				partInStrm.close();

				logger.info("Loaded " + baseFilename + ".partition." + newParts[i]);
			}
		}

		for (int i = 0; i < loadedparts.length; i++) {
			int partId = loadedparts[i];
			LoadedVertexInterval interval = new LoadedVertexInterval(PartitionQuerier.getFirstSrc(partId),
					PartitionQuerier.getLastSrc(partId), partId);
			interval.setIndexStart(indexSt);
			indexEd = indexEd + PartitionQuerier.getNumUniqueSrcs(partId) - 1;
			interval.setIndexEnd(indexEd);
			intervals.add(interval);
			indexSt = indexEd + 1;
		}

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

		for (int i = 0; i < intervals.size(); i++) {

			// locate the required interval in "vertices", and if it has new
			// edges added
			if (partitionId == intervals.get(i).getPartitionId() & intervals.get(i).hasNewEdges()) {

				// clear current file
				DataOutputStream partOutStrm = new DataOutputStream(new BufferedOutputStream(
						new FileOutputStream(GlobalParams.baseFilename + ".partition." + partitionId, false)));
				partOutStrm.close();

				partOutStrm = new DataOutputStream(new BufferedOutputStream(
						new FileOutputStream(GlobalParams.baseFilename + ".partition." + partitionId, true)));

				int srcVId, destVId, count;
				int edgeValue;

				// scan each vertex in this interval in "vertices" datastructure
				for (int j = intervals.get(i).getIndexStart(); j < intervals.get(i).getIndexEnd() + 1; j++) {

					// write the srcId
					srcVId = vertices[j].getVertexId();
					partOutStrm.writeInt(srcVId);

					// write the count
					count = vertices[j].getCombinedDeg();
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
				partOutStrm.close();
			}

		}

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

		for (int i = 0; i < intervals.size(); i++) {

			// locate the required interval in "vertices", and if it has new
			// edges added
			if (partitionId == intervals.get(i).getPartitionId() & intervals.get(i).hasNewEdges()) {

				// clear current degrees file
				PrintWriter partDegOutStrm = new PrintWriter(new BufferedWriter(
						new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId + ".degrees", false)));
				partDegOutStrm.close();

				partDegOutStrm = new PrintWriter(new BufferedWriter(
						new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId + ".degrees", true)));

				int srcVId, deg;

				// scan each vertex in this interval in "vertices" datastructure
				for (int j = intervals.get(i).getIndexStart(); j < intervals.get(i).getIndexEnd() + 1; j++) {

					// get srcId and deg
					srcVId = vertices[j].getVertexId();
					deg = vertices[j].getCombinedDeg();
					partDegOutStrm.println(srcVId + "\t" + deg);

				}
				partDegOutStrm.close();
			}
		}

	}
}
