package edu.uci.ics.cs.graspan.preproc;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

public class GraphERuleEdgeAdder {

	private static final Logger logger = GraspanLogger.getLogger("GraphERuleEdgeAdder");

	public static Vertex[] vertices;
	private String baseFilename;
	private long numEdges;
	
	int[][][] partEdges;
	byte[][][] partEdgeVals;
	
	int partOutDegs[][] ;
	// for tracking progress after processing every
	// "OUTPUT_EDGE_TRACKER_INTERVAL" edges
	private static final long OUTPUT_EDGE_TRACKER_INTERVAL = 500000;

	// map of vertices and degrees
	private TreeMap<Integer, Integer> outDegs;

	public GraphERuleEdgeAdder() throws NumberFormatException, IOException {

		this.baseFilename = GlobalParams.getBasefilename();

		// get the partition allocation table
		// this.readPartAllocTable();
		// logger.info("Loaded " + baseFilename + ".partAllocTable");

		// get the grammar info
//		GrammarChecker.loadGrammars(new File(baseFilename + ".grammar"));
		logger.info("Loaded " + baseFilename + ".grammar");

		preliminaryInit();
	
	}

	private void preliminaryInit() {

		// we shall load, process, and save the graph //WE CONSIDER THE ENTIRE
		// GRAPH AS ONE PARTITION
		int loadedPartOutDegs[][] = new int[1][];
		int loadedPartEdges[][][] = new int[1][][];
		byte loadedPartEdgeVals[][][] = new byte[1][][];

		this.partOutDegs = loadedPartOutDegs;
		this.partEdges = loadedPartEdges;
		this.partEdgeVals = loadedPartEdgeVals;

	}

	/**
	 * description:
	 * 
	 * @param partId
	 * @throws IOException
	 */
	public void run() throws IOException {

		logger.info("ADDING ERULE EDGES TO GRAPH");

		long degGenStartTime = System.nanoTime();
		generateGraphDegs(new FileInputStream(new File(GlobalParams.getBasefilename())));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		logger.info("Total time to create degrees file of original graph (nanoseconds): " + degGenDuration);

		readDegrees();
		logger.info("Loaded " + baseFilename + ".degrees");

		initVarsOfPartsToLoad();
		logger.info("Initialized data structures for partition to load.");

		// fill the partition data structures
		loadPartData();

		//this is not updated because it is unnecessary later

		logger.info("Loaded " + baseFilename);
		
		
//		for (int i=0;i<vertices.length;i++){
//			logger.info(vertices[i].getVertexId()+" "+Arrays.toString(vertices[i].getOutEdges()));
//			}


		addEdgesforERules();
		logger.info("Added new edges for ERules for " + baseFilename);

		sort();
		logger.info("Sorted " + baseFilename);

		save();
		logger.info("Saved data (adjacency lists & degrees) for " + baseFilename);
	}

	private void addEdgesforERules() {
		// add edges corresponding to epsilon rules.
		Set<Byte> eRules = GrammarChecker.eRules;

		int srcId = 0;

		// for each loaded source vertex row
		for (int i = 0; i < vertices.length; i++) {

			srcId = vertices[i].getVertexId();

			// first assume all eRules values need to be added
			HashSet<Byte> newValsforSrc = new HashSet<Byte>(eRules);

			// find values already existing for this source row, and remove
			// those values from newValsforSrc
			removeExistingERuleVals(srcId, i, newValsforSrc);

			addNewEdges(srcId, i, newValsforSrc);
		}
	}

	private void addNewEdges(int srcId, int i, HashSet<Byte> newValsforSrc) {
		int[] tempEdgs;
		byte[] tempVals;
		// add the new edges to tempEdgs
		tempEdgs = new int[vertices[i].getOutEdges().length + newValsforSrc.size()];
		tempVals = new byte[vertices[i].getOutEdges().length + newValsforSrc.size()];

//		assert (srcId != 0);

		int tempArrMarker = 0;
		for (Byte eval : newValsforSrc) {
			tempEdgs[tempArrMarker] = srcId;
			tempVals[tempArrMarker] = eval;
			tempArrMarker++;
		}

		System.arraycopy(vertices[i].getOutEdges(), 0, tempEdgs, tempArrMarker, vertices[i].getOutEdges().length);
		System.arraycopy(vertices[i].getOutEdgeValues(), 0, tempVals, tempArrMarker, vertices[i].getOutEdgeValues().length);

		// reset the outEdges/outVals
		vertices[i].setOutEdges(tempEdgs);
		vertices[i].setOutEdgeValues(tempVals);
	}

	private void removeExistingERuleVals(int srcId, int i, HashSet<Byte> newValsforSrc) {
		int destId;
		for (int j = 0; j < vertices[i].getOutEdges().length; j++) {
			destId = vertices[i].getOutEdge(j);
			if (destId > srcId)
				break;
			newValsforSrc.remove(vertices[i].getOutEdgeValue(j));
		}
	}

	/**
	 * 
	 * @param loadedPartOutDegs
	 * @param partEdges
	 * @param partEdgeVals
	 */
	private void sort() {
//		for (int j = 0; j < partEdges.length; j++) {
		for (int j = 0 ; j < vertices.length ; j++) {
			int low = 0;
//			int high = partOutDegs[0][j] - 1;
			int high = vertices[j].getOutEdges().length;
			Utilities.quickSort(vertices[j].getOutEdges(), vertices[j].getOutEdgeValues(), low, high);
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
		int partAllocTable[][] = new int[1][2];

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
	}

	public Vertex[] getVertices() {
		return vertices;
	}

	/**
	 * 
	 * @param partitionId
	 * @throws IOException
	 */
	private void save() throws IOException {
		storePart_ActualEdges();
		// storePartBin(getVertices(), partitionId);
		// storePartDegs(getVertices(), partitionId);
	}

	private void readDegrees() throws IOException {

		partOutDegs[0] = new int[outDegs.lastKey()-outDegs.firstKey()+1];
		/*
		 * Scan degrees file of partition
		 */
		BufferedReader outDegInStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename + ".degrees"))));

		String ln;
		while ((ln = outDegInStrm.readLine()) != null) {

			String[] tok = ln.split("\t");

			// get the srcVId and degree
			int srcVId = Integer.parseInt(tok[0]);
			int deg = Integer.parseInt(tok[1]);
//			try {
				if (GlobalParams.getFirstVertexID() == 0) {
//					logger.info(outDegs.size()+" "+srcVId);
					partOutDegs[0][srcVId] = deg;
				}
				if (GlobalParams.getFirstVertexID() == 1) {
					partOutDegs[0][srcVId - 1] = deg;
				}
//				partOutDegs[0][srcVId - outDegs.lastKey()] = deg;
//			} catch (Exception e) {
//				logger.info("ERROR!: " + srcVId + " " + outDegs.lastKey());
//			}
		}
		outDegInStrm.close();
	}

	/**
	 * Initializes data structures of the partitions to load (In this
	 * implementation, only one partition is loaded at a time)
	 */
	private void initVarsOfPartsToLoad() {

		// initializing new data structures
		int totalNumVertices = 0;
		totalNumVertices = outDegs.lastKey()-outDegs.firstKey()+1;

		vertices = new Vertex[totalNumVertices];

		// reassigning the partEdges and partEdgeVals

		// initialize Dimension 2 (Total no. of Unique SrcVs for a
		// Partition)
		partEdges[0] = new int[outDegs.lastKey()-outDegs.firstKey()+1][];
		partEdgeVals[0] = new byte[outDegs.lastKey()-outDegs.firstKey()+1][];

		for (int j = 0; j < partEdges[0].length; j++) {
			// initialize Dimension 3 (Total no. of Out-edges for a SrcV)
			partEdges[0][j] = new int[partOutDegs[0][j]];
			partEdgeVals[0][j] = new byte[partOutDegs[0][j]];

		}

		// set vertices data structure
		int vertexId = 0;
		for (int j = 0; j < vertices.length; j++) {
			if (GlobalParams.getFirstVertexID() == 0) {
				vertexId = j;
			}
			if (GlobalParams.getFirstVertexID() == 1) {
				vertexId = j + 1;
			}
			vertices[j] = new Vertex(j, vertexId, partEdges[0][j], partEdgeVals[0][j]);
		}
	}

	public void loadPartData() throws IOException {
//		logger.info(outDegs+"the out degrees");
		
		BufferedReader partInStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(baseFilename))));
		
		// stores the position of last filled edge (destV) and the edge
		// val in partEdges and partEdgeVals for a source vertex for a partition
		int[] lastAddedEdgePos = new int[outDegs.lastKey()-outDegs.firstKey()+1];
		for (int j = 0; j < lastAddedEdgePos.length; j++) {
			lastAddedEdgePos[j] = -1;
		}
		String ln;
		while ((ln = partInStrm.readLine()) != null) {
			if (!ln.isEmpty()){
			
			String[] tok = ln.split("\t");
			
			
			int src = Integer.parseInt(tok[0]);
//			logger.info("Input Line " + Arrays.toString(tok));

			int arraySrcVId = 0;

			// get corresponding arraySrcVId of srcVId
			if (GlobalParams.getFirstVertexID() == 0) {
				arraySrcVId = src;
			}
			if (GlobalParams.getFirstVertexID() == 1) {
				arraySrcVId = src - 1;
			}
			
//			logger.info(" arraySrcVId "+arraySrcVId+" lastAddedEdgePos "+lastAddedEdgePos.length);
			
			// dstVId
//			logger.info(
//					"arraySrcVId " + arraySrcVId + " lastAddedEdgePos[arraySrcVId]  " + lastAddedEdgePos[arraySrcVId]);
			partEdges[0][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = Integer.parseInt(tok[1]);

			// edgeVal
//			logger.info(
//					"arraySrcVId " + arraySrcVId + " lastAddedEdgePos[arraySrcVId] " + lastAddedEdgePos[arraySrcVId]);
			partEdgeVals[0][arraySrcVId][lastAddedEdgePos[arraySrcVId] + 1] = GrammarChecker.getValue(tok[2]);;

			// increment the last added position for this row
			lastAddedEdgePos[arraySrcVId]++;
		}
		}

//		logger.info("partEdges: "+Arrays.deepToString(partEdges));

		partInStrm.close();

	}

	/**
	 * Stores a partition to disk.
	 * 
	 * @param vertices
	 * @param partitionId
	 * @throws IOException
	 */
	private static void storePartBin(Vertex[] vertices, Integer partitionId) throws IOException {

		logger.info("Updating " + GlobalParams.baseFilename);

		// clear current file
		DataOutputStream partOutStrm = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(GlobalParams.baseFilename, false)));
		partOutStrm.close();

		partOutStrm = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(GlobalParams.baseFilename, true)));

		int srcVId, destVId, count;
		int edgeValue;
		
		for (int j = 0; j < vertices.length; j++) {
			count = vertices[j].getNumOutEdges();
			
			logger.info(count+"");

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
	
	private void storePart_ActualEdges() throws IOException {
		
		logger.info("Updating " + GlobalParams.baseFilename);
		
//		for (int i=0;i<vertices.length;i++){
//		logger.info(vertices[i].getVertexId()+" "+Arrays.toString(vertices[i].getOutEdges()));
//		}
		
		
		// clear current graph file
		PrintWriter partOutStrm = new PrintWriter(new BufferedWriter(
				new FileWriter(GlobalParams.baseFilename , false)));
		partOutStrm.close();

		partOutStrm = new PrintWriter(new BufferedWriter(
				new FileWriter(GlobalParams.baseFilename , true)));

		int srcVId, destVId, count;
		int edgeValue;
		String edgeValStr="";
		
		
		for (int j = 0; j < vertices.length; j++) {
			
			count = vertices[j].getNumOutEdges();
			if (count == 0) {
				continue;
			}

			// write the srcId
			srcVId = vertices[j].getVertexId();

			// scan each edge (original edge) in list of each vertex in
			// this interval
			for (int k = 0; k < vertices[j].getNumOutEdges(); k++) {

				// write the destId-edgeValue pair
				if (vertices[j].getOutEdges().length > 0) {
					if (vertices[j].getOutEdge(k) == -1)
						break;
					destVId = vertices[j].getOutEdge(k);
					edgeValue = vertices[j].getOutEdgeValue(k);
					edgeValStr=GrammarChecker.getValue((byte)edgeValue);
					partOutStrm.println(srcVId + "\t" + destVId+ "\t" + edgeValStr);
				}
			}
		}

		partOutStrm.close();
		
	}

	/**
	 * Scans the entire graph and counts the out-degrees of all the vertices.
	 * This is the Preliminary Scan | TODO need to store the degrees file in a
	 * more space-efficient way
	 * 
	 * @param inputStream
	 * @param format
	 * @throws IOException
	 */
	public void generateGraphDegs(InputStream inputStream) throws IOException {
		logger.info("Generating degrees file...");
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long numEdges = 0;
		TreeMap<Integer, Integer> outDegs = new TreeMap<Integer, Integer>();

		// read inputgraph line-by-line and keep incrementing degree
		logger.info("Performing first scan on input graph... ");
		long lineCount = 0;
		long readStartTime = System.nanoTime();
		int src = 0;
		double readSpeed = 0;
		String[] tok;

		while ((ln = ins.readLine()) != null) {
			lineCount++;
			if (lineCount % OUTPUT_EDGE_TRACKER_INTERVAL == 0) {
				logger.info("Reading edge #" + NumberFormat.getNumberInstance(Locale.US).format(lineCount) + ".");
				readSpeed = OUTPUT_EDGE_TRACKER_INTERVAL * 1000000000 / ((System.nanoTime() - readStartTime));
				logger.info("Read speed: " + readSpeed + " edges/sec");
				readStartTime = System.nanoTime();
			}
			tok = ln.split("\t");
			try {
					src = Integer.parseInt(tok[0]);

				if (!outDegs.containsKey(src)) {
					outDegs.put(src, 1);
				} else {
					outDegs.put(src, outDegs.get(src) + 1);
				}
				numEdges++;
//				logger.info(numEdges+" Num edges");
			} catch (Exception e) {
				logger.info("ERROR: " + e + "at line # " + lineCount + " : " + ln);
			}
		}
		logger.info("Completed first scan of input graph and got full degree information in the memory.");

		this.numEdges = numEdges;
		logger.info("Total number of edges in input graph: " + numEdges);

		// Save the degrees on disk
		logger.info("Saving degrees file " + baseFilename + ".degrees... ");

		PrintWriter outDegOutStrm = new PrintWriter(baseFilename + ".degrees", "UTF-8");
		Iterator<Entry<Integer, Integer>> it = outDegs.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Integer, Integer> pair = it.next();
			outDegOutStrm.println(pair.getKey() + "\t" + pair.getValue());
		}
		outDegOutStrm.close();
		this.outDegs = outDegs;

		logger.info("Completed saving all degrees files.");
		// logger.info(outDegs+"");
	}

	// /**
	// * Stores degrees of a partition.
	// *
	// * @param vertices
	// * @param partitionId
	// * @throws IOException
	// */
	// public static void storePartDegs(Vertex[] vertices, Integer partitionId)
	// throws IOException {
	//
	// logger.info("Updating " + GlobalParams.baseFilename + ".partition." +
	// partitionId + ".degrees");
	//
	// // clear current degrees file
	// PrintWriter partDegOutStrm = new PrintWriter(new BufferedWriter(
	// new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId +
	// ".degrees", false)));
	// partDegOutStrm.close();
	//
	// partDegOutStrm = new PrintWriter(new BufferedWriter(
	// new FileWriter(GlobalParams.baseFilename + ".partition." + partitionId +
	// ".degrees", true)));
	//
	// int srcVId, deg;
	//
	// // scan each vertex in this interval in "vertices" datastructure
	// for (int j = 0; j < vertices.length; j++) {
	// // get srcId and deg
	// srcVId = vertices[j].getVertexId();
	// deg = vertices[j].getNumOutEdges();
	// if (deg == 0)
	// continue;
	// partDegOutStrm.println(srcVId + "\t" + deg);
	// }
	// partDegOutStrm.close();
	// }
}
