package edu.uci.ics.cs.graspan.preproc;

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
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.LoadedPartitions;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.PartitionQuerier;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

public class PartitionPreprocessor {
	
	private static final Logger logger = GraspanLogger.getLogger("Loader");

	public static Vertex[] vertices;
	private String baseFilename;
	private int numParts;

	public PartitionPreprocessor() throws NumberFormatException, IOException {

		this.baseFilename = GlobalParams.getBasefilename();
		this.numParts = GlobalParams.getNumParts();

		// get the partition allocation table
		this.readPartAllocTable();

		// get the grammar info
		GrammarChecker.loadGrammars(new File(baseFilename + ".grammar"));

		preliminaryInit();
	}

	private void preliminaryInit() {

		// we shall load, process, and save one partition at a time
		int loadedPartOutDegs[][] = new int[1][];
		int loadedPartEdges[][][] = new int[1][][];
		byte loadedPartEdgeVals[][][] = new byte[1][][];

		LoadedPartitions.setLoadedPartOutDegs(loadedPartOutDegs);
		LoadedPartitions.setLoadedPartEdges(loadedPartEdges);
		LoadedPartitions.setLoadedPartEdgeVals(loadedPartEdgeVals);

	}

	public void loadAndProcessParts(int partId) throws IOException {

		logger.info("PREPROCESSING PARTITION : " + partId + "...");
		readDegrees(partId);
		initVarsOfPartsToLoad(partId);

		logger.info("Initialized data structures for partitions to load.");

		// fill the partition data structures
		loadPartData(partId);

		int loadedPartOutDegs[][] = LoadedPartitions.getLoadedPartOutDegs();
		int partEdges[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeVals[][][] = LoadedPartitions.getLoadedPartEdgeVals();

		sortPart(partId, loadedPartOutDegs, partEdges, partEdgeVals);

		savePartAndDegs(partId);
		
		}

	/**
	 * 
	 * @param partId
	 * @param loadedPartOutDegs
	 * @param partEdges
	 * @param partEdgeVals
	 */
	private void sortPart(int partId, int[][] loadedPartOutDegs, int[][][] partEdges, byte[][][] partEdgeVals) {
		for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partId); j++) {
			int low = 0;
			int high = loadedPartOutDegs[0][j] - 1;
			Utilities.quickSort(partEdges[0][j], partEdgeVals[0][j], low, high);
		}
		logger.info("Sorted loaded partition.");
	}

	/**
	 * Gets the partition allocation table. 
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
		BufferedReader inPartAllocTabStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(baseFilename+ ".partAllocTable"))));
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

	public Vertex[] getVertices() {
		return vertices;
	}

	
	/**
	 * 
	 * @param partitionId
	 * @throws IOException
	 */
	private void savePartAndDegs(int partitionId) throws IOException {
		storePart(getVertices(), partitionId);
		storePartDegs(getVertices(), partitionId);
	}

	
	private void readDegrees(int partId) throws IOException {

		int[][] partOutDegs = LoadedPartitions.getLoadedPartOutDegs();

		partOutDegs[0] = new int[PartitionQuerier.getNumUniqueSrcs(partId)];

		/*
		 * Scan degrees file of partition
		 */
		BufferedReader outDegInStrm = new BufferedReader(new InputStreamReader(
				new FileInputStream(new File(baseFilename + ".partition." + partId + ".degrees"))));

		String ln;
		while ((ln = outDegInStrm.readLine()) != null) {

			String[] tok = ln.split("\t");

			// get the srcVId and degree
			int srcVId = Integer.parseInt(tok[0]);
			int deg = Integer.parseInt(tok[1]);
			try {
				partOutDegs[0][srcVId - PartitionQuerier.getFirstSrc(partId)] = deg;
			} catch (Exception e) {
				logger.info("ERROR!: " + srcVId + " " + PartitionQuerier.getFirstSrc(partId));
			}
		}
		outDegInStrm.close();

		logger.info("Loaded " + baseFilename + ".partition." + partId + ".degrees");
	}

	/**
	 * Initializes data structures of the partitions to load
	 */
	private void initVarsOfPartsToLoad(int partId) {

		int[][] partOutDegs = LoadedPartitions.getLoadedPartOutDegs();

		// initializing new data structures
		int totalNumVertices =  PartitionQuerier.getNumUniqueSrcs(partId);
		vertices = new Vertex[totalNumVertices];

		int partEdges[][][] = LoadedPartitions.getLoadedPartEdges();
		byte partEdgeVals[][][] = LoadedPartitions.getLoadedPartEdgeVals();

		// reassigning the partEdges and partEdgeVals

		// initialize Dimension 2 (Total no. of Unique SrcVs for a
		// Partition)
		partEdges[0] = new int[PartitionQuerier.getNumUniqueSrcs(partId)][];
		partEdgeVals[0] = new byte[PartitionQuerier.getNumUniqueSrcs(partId)][];

		for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partId); j++) {
			// initialize Dimension 3 (Total no. of Out-edges for a SrcV)
			partEdges[0][j] = new int[partOutDegs[0][j]];
			partEdgeVals[0][j] = new byte[partOutDegs[0][j]];

		}

		// set vertices data structure
		int vertexIdx = 0;
			for (int j = 0; j < PartitionQuerier.getNumUniqueSrcs(partId); j++) {
				int vertexId = PartitionQuerier.getActualIdFrmPartArrIdx(j, partId);
				vertices[vertexIdx] = new Vertex(vertexIdx, vertexId, partEdges[0][j], partEdgeVals[0][j]);
				vertexIdx++;
			}
	}

	private void loadPartData(int partId) throws IOException {

		int[][][] partEdges = LoadedPartitions.getLoadedPartEdges();
		byte[][][] partEdgeVals = LoadedPartitions.getLoadedPartEdgeVals();

				DataInputStream partInStrm = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partId)));

				// stores the position of last filled edge (destV) and the edge
				// val in partEdges and partEdgeVals for a source vertex for a partition
				int[] lastAddedEdgePos = new int[PartitionQuerier.getNumUniqueSrcs(partId)];
				for (int j = 0; j < lastAddedEdgePos.length; j++) {
					lastAddedEdgePos[j] = -1;
				}

				while (partInStrm.available() != 0) {
					{
						try {
							// get srcVId
							int src = partInStrm.readInt();

							// get corresponding arraySrcVId of srcVId
							int arraySrcVId = src - PartitionQuerier.getFirstSrc(partId);

							// get count (number of destVs from srcV in the
							// current list of the partition file)
							int count = partInStrm.readInt();

							// get dstVId & edgeVal and store them in the corresponding arrays
							for (int j = 0; j < count; j++) {

								// dstVId
								partEdges[0][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partInStrm.readInt();

 								// edgeVal
								partEdgeVals[0][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = partInStrm.readByte();

								// increment the last added position for this row
								lastAddedEdgePos[arraySrcVId]++;
							}

						} catch (Exception exception) {
							break;
						}
					}
				}

				partInStrm.close();

				logger.info("Loaded " + baseFilename + ".partition."+ partId);


	}

	/**
	 * Stores a partition to disk.
	 * @param vertices
	 * @param partitionId
	 * @throws IOException
	 */
	private static void storePart(Vertex[] vertices, Integer partitionId)
			throws IOException {

		logger.info("Updating " + GlobalParams.baseFilename + ".partition." + partitionId);

		// clear current file
		DataOutputStream partOutStrm = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(GlobalParams.baseFilename + ".partition." + partitionId, false)));
		partOutStrm.close();

		partOutStrm = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(GlobalParams.baseFilename + ".partition." + partitionId, true)));

		int srcVId, destVId, count;
		int edgeValue;

		for (int j = 0; j < vertices.length; j++) {
			count = vertices[j].getNumOutEdges();

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
				if (vertices[j].getOutEdges().length > 0) {
					if (vertices[j].getOutEdge(k) == -1)
						break;
					destVId = vertices[j].getOutEdge(k);
					edgeValue = vertices[j].getOutEdgeValue(k);
					partOutStrm.writeInt(destVId);
					partOutStrm.writeByte(edgeValue);
				}
			}
		}
		partOutStrm.close();
	}

	/**
	 * Stores degrees of a partition.
	 * @param vertices
	 * @param partitionId
	 * @throws IOException
	 */
	public static void storePartDegs(Vertex[] vertices, Integer partitionId) throws IOException {

		logger.info("Updating " + GlobalParams.baseFilename + ".partition." + partitionId + ".degrees");

		// clear current degrees file
		PrintWriter partDegOutStrm = new PrintWriter(new BufferedWriter(
				new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId + ".degrees", false)));
		partDegOutStrm.close();

		partDegOutStrm = new PrintWriter(new BufferedWriter(
				new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId + ".degrees", true)));

		int srcVId, deg;

		// scan each vertex in this interval in "vertices" datastructure
		for (int j = 0; j < vertices.length; j++) {
			// get srcId and deg
			srcVId = vertices[j].getVertexId();
			deg = vertices[j].getNumOutEdges();
			if (deg == 0)
				continue;
			partDegOutStrm.println(srcVId + "\t" + deg);
		}
		partDegOutStrm.close();
	}
}
