package edu.uci.ics.cs.gdtc.partitionloader;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Logger;

import edu.uci.ics.cs.gdtc.partitiondata.AllPartitions;
import edu.uci.ics.cs.gdtc.partitiondata.LoadedPartitions;
import edu.uci.ics.cs.gdtc.partitiondata.LoadedVertexInterval;
import edu.uci.ics.cs.gdtc.partitiondata.PartitionQuerier;
import edu.uci.ics.cs.gdtc.partitiondata.Vertex;
import edu.uci.ics.cs.gdtc.scheduler.SchedulerInfo;
import edu.uci.ics.cs.gdtc.support.GDTCLogger;
import edu.uci.ics.cs.gdtc.support.Optimizers;
import edu.uci.ics.cs.gdtc.userinput.UserInput;

/**
 * This program loads partitions into the memory.
 * 
 * @author Aftab
 *
 */
public class PartitionLoader {

	private static final Logger logger = GDTCLogger.getLogger("graphdtc partitionloader");

	private Vertex[] vertices = null;
	private ArrayList<LoadedVertexInterval> intervals = new ArrayList<LoadedVertexInterval>();

	private String baseFilename = "";

	private int numParts = 0;

	/**
	 * Initializes the partition loader, reads in the partition allocation
	 * table, and reads in the edgedestcountInfo
	 * 
	 * @param baseFilename
	 * @param numParts
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public PartitionLoader() throws NumberFormatException, IOException {

		this.baseFilename = UserInput.getBasefilename();
		this.numParts = UserInput.getNumParts();

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

		int newParts[] = new int[UserInput.getNumPartsPerComputation()];
		int loadedParts[] = new int[UserInput.getNumPartsPerComputation()];

		for (int i = 0; i < loadedParts.length; i++) {
			newParts[i] = -1;
			loadedParts[i] = -1;
		}

		int loadedPartOutDegs[][] = new int[UserInput.getNumPartsPerComputation()][];
		int loadedPartEdges[][][] = new int[UserInput.getNumPartsPerComputation()][][];
		byte loadedPartEdgeVals[][][] = new byte[UserInput.getNumPartsPerComputation()][][];

		LoadedPartitions.setLoadedParts(newParts);
		LoadedPartitions.setLoadedParts(loadedParts);
		LoadedPartitions.setLoadedPartOutDegs(loadedPartOutDegs);
		LoadedPartitions.setLoadedPartEdges(loadedPartEdges);
		LoadedPartitions.setLoadedPartEdgeVals(loadedPartEdgeVals);
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
		updateNewParts(partsToLoad);

		// update loadedPartOutDegrees
		updateDegsOfPartsToLoad();

		// initialize data structures of the partitions to load
		initVarsOfPartsToLoad();

		logger.info("Initialized data structures for partitions to load.");
		// fill the partition data structures
		fillVarsOfPartsToLoad(partsToLoad);

		int loadedPartOutDegs[][] = LoadedPartitions.getLoadedPartOutDegs();
		int partEdges[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeVals[][][] = LoadedPartitions.getLoadedPartEdgeVals();

		// sorting the partitions
		for (int i = 0; i < partsToLoad.length; i++) {
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partsToLoad[i]); j++) {
				int low = 0;
				int high = loadedPartOutDegs[i][j] - 1;
				Optimizers.quickSort(partEdges[i][j], partEdgeVals[i][j], low, high);
			}
		}
		logger.info("Sorted loaded partitions.");

		LoadedPartitions.setLoadedParts(partsToLoad);

		// loaded partitions test
		// LoadedPartitions.printData();
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
		int partAllocTable[] = new int[numParts];

		/*
		 * Scan the partition allocation table file
		 */
		BufferedReader inPartAllocTabStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".partAllocTable"))));
		String ln, tok;

		int i = 0;
		while ((ln = inPartAllocTabStrm.readLine()) != null) {
			tok = ln;
			// store partition allocation table in memory
			partAllocTable[i] = Integer.parseInt(tok);
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
		logger.info("Loaded part. sizes file " + baseFilename + ".partSizes");

	}

	public Vertex[] getVertices() {
		return vertices;
	}

	public ArrayList<LoadedVertexInterval> getIntervals() {
		return intervals;
	}

	private void updateNewParts(int partsToLoad[]) {
		/*
		 * Initialize the degrees array for each new partition to load. We shall
		 * not reinitialize the degrees of the partitions that have already been
		 * loaded. In addition, at no point will partsToLoad be equal to
		 * loadedparts
		 */
		int[] loadedParts = LoadedPartitions.getLoadedParts();
		int[] newParts = LoadedPartitions.getNewParts();

		/*
		 * partid loading test 1/3 (comment loadedParts, newParts initialization
		 * above, change name of parameter partsToLoad above)
		 * 
		 */
		// int[] loadedParts = { 5, 7, 10, 1, 101 };
		// int[] newParts = { -1, -1, -1, -1, -1 };
		// int[] partsToLoad = { 6, 5, 7, 10, 2 };
		// System.out.println("START");
		// System.out.println("loadedParts");
		// for (int i = 0; i < loadedParts.length; i++)
		// System.out.print(loadedParts[i] + " ");
		// System.out.println();
		// System.out.println("newParts");
		// for (int i = 0; i < newParts.length; i++)
		// System.out.print(newParts[i] + " ");
		// System.out.println();
		// System.out.println("partsToLoad");
		// for (int i = 0; i < partsToLoad.length; i++)
		// System.out.print(partsToLoad[i] + " ");
		// System.out.println();

		// check whether the requested partition for loading is already loaded
		for (int i = 0; i < partsToLoad.length; i++) {
			for (int j = 0; j < loadedParts.length; j++) {
				if (partsToLoad[i] == loadedParts[j]) {
					// clear parts already loaded in partsToLoad
					partsToLoad[i] = -1;
					// mark old parts in loadedParts
					loadedParts[j] = -loadedParts[j];
				}
			}
		}

		/*
		 * partid loading test 2/3
		 */
		// System.out.println("AFTER MARKING NEW PART IDS");
		// System.out.println("loadedParts");
		// for (int i = 0; i < loadedParts.length; i++)
		// System.out.print(loadedParts[i] + " ");
		// System.out.println();
		// System.out.println("newParts");
		// for (int i = 0; i < newParts.length; i++)
		// System.out.print(newParts[i] + " ");
		// System.out.println();
		// System.out.println("partsToLoad");
		// for (int i = 0; i < partsToLoad.length; i++)
		// System.out.print(partsToLoad[i] + " ");
		// System.out.println();

		// store the ids of the new partitions to load in newParts to preserve
		// order. (Note: order is important as other variables like
		// loadedPartdegs, partEdges, etc use order to store values.)
		for (int i = 0; i < partsToLoad.length; i++) {
			if (partsToLoad[i] != -1) {
				// found new partid
				for (int j = 0; j < loadedParts.length; j++) {
					if (loadedParts[j] >= 0) {
						// found loaded partid to be discarded
						loadedParts[j] = -loadedParts[j];
						// store new partid in the corresponding index in
						// newParts
						newParts[j] = partsToLoad[i];
						break;
					}
				}
			}
		}

		// getting newparts in loadedparts
		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != -1) {
				loadedParts[i] = newParts[i];
			}
			if (loadedParts[i] < 0) {
				loadedParts[i] = -loadedParts[i];
			}
		}

		/*
		 * partid loading test 3/3
		 */
		// System.out.println("AFTER STORING NEW PARTS");
		// System.out.println("loadedParts");
		// for (int i = 0; i < loadedParts.length; i++)
		// System.out.print(loadedParts[i] + " ");
		// System.out.println();
		// System.out.println("newParts");
		// for (int i = 0; i < newParts.length; i++)
		// System.out.print(newParts[i] + " ");
		// System.out.println();
		// System.out.println("partsToLoad");
		// for (int i = 0; i < partsToLoad.length; i++)
		// System.out.print(partsToLoad[i] + " ");
		// System.out.println();
		// System.exit(0);

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

		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != -1) {
				// initialize Dimension 2 (Total no. of Unique SrcVs for a
				// Partition)
				partOutDegs[i] = new int[PartitionQuerier.getNumUniqueSrcs(newParts[i])];
			}
		}

		/*
		 * Scan degrees file of each partition
		 */
		for (int i = 0; i < newParts.length; i++) {
			BufferedReader outDegInStrm = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File(baseFilename + ".partition." + newParts[i] + ".degrees"))));

			String ln;
			while ((ln = outDegInStrm.readLine()) != null) {

				String[] tok = ln.split("\t");

				// get the srcVId and degree
				int srcVId = Integer.parseInt(tok[0]);
				int deg = Integer.parseInt(tok[1]);

				partOutDegs[i][srcVId - PartitionQuerier.getMinSrc(newParts[i])] = deg;
			}
			outDegInStrm.close();

			logger.info("Loaded " + baseFilename + ".partition." + newParts[i] + ".degrees" + newParts[i]);
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
						partOutDegs[i][srcVId - PartitionQuerier.getMinSrc(partsToLoad[i])] = deg;
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

		// initializing new data structures
		int totalNumVertices = 0;
		for (int i = 0; i < loadedParts.length; i++) {
			totalNumVertices += PartitionQuerier.getNumUniqueSrcs(loadedParts[i]);
		}
		vertices = new Vertex[totalNumVertices];

		// initialize Dimension 1 (Total no. of Partitions)
		int partEdges[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeVals[][][] = LoadedPartitions.getLoadedPartEdgeVals();

		int count = 0;
		for (int i = 0; i < newParts.length; i++) {
			if (newParts[i] != -1) {

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

		for (int i = 0; i < loadedParts.length; i++) {
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(loadedParts[i]); j++) {
				int vertexId = PartitionQuerier.getActualIdFrmPartArrId(j, loadedParts[i]);
				vertices[count++] = new Vertex(vertexId, partEdges[i][j], partEdgeVals[i][j]);
			}
		}

	}

	/**
	 * Reads the partition files and stores them in arrays
	 * 
	 * @param partInputStream
	 * @throws IOException
	 */
	private void fillVarsOfPartsToLoad(int[] partsToLoad) throws IOException {

		int[][][] partEdges = LoadedPartitions.getLoadedPartEdges();
		byte[][][] partEdgeVals = LoadedPartitions.getLoadedPartEdgeVals();

		int indexSt = 0;
		int indexEd = 0;

		for (int i = 0; i < partsToLoad.length; i++) {

			DataInputStream partInStrm = new DataInputStream(
					new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partsToLoad[i])));

			// stores the position of last filled edge (destV) and the edge val
			// in partEdges and partEdgeVals for a source vertex
			// for a partition
			int[] lastAddedEdgePos = new int[PartitionQuerier.getNumUniqueSrcs(partsToLoad[i])];
			for (int j = 0; j < lastAddedEdgePos.length; j++) {
				lastAddedEdgePos[j] = -1;
			}

			while (partInStrm.available() != 0) {
				{
					try {
						// get srcVId
						int src = partInStrm.readInt();

						// get corresponding arraySrcVId of srcVId
						int arraySrcVId = src - PartitionQuerier.getMinSrc(partsToLoad[i]);

						// get count (number of destVs from srcV in the current
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

							// increment the last added position for this row
							lastAddedEdgePos[arraySrcVId]++;
						}

					} catch (Exception exception) {
						break;
					}
				}
			}

			int partId = partsToLoad[i];
			LoadedVertexInterval interval = new LoadedVertexInterval(PartitionQuerier.getMinSrc(partId),
					PartitionQuerier.getMaxSrc(partId), partId);
			interval.setIndexStart(indexSt);
			indexEd = indexEd + PartitionQuerier.getNumUniqueSrcs(partId) - 1;
			interval.setIndexEnd(indexEd);
			intervals.add(interval);
			indexSt = indexEd + 1;
			partInStrm.close();

			logger.info("Loaded " + baseFilename + ".partition." + partsToLoad[i]);
		}
	}
}
