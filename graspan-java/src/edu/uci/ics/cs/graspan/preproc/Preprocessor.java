package edu.uci.ics.cs.graspan.preproc;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.PartitionQuerier;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.scheduler.SchedulerInfo;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * 
 * @author Aftab
 * 
 */
public class Preprocessor {

	private static final Logger logger = GraspanLogger.getLogger("Preprocessor");

	// number of input partitions
	private int numParts;

	// total number of input edges
	private long numEdges;

	// path of the input graph file
	private String baseFilename;

	// map of vertices and degrees
	private TreeMap<Integer, Integer> outDegs;

	// the total number of edges that can be kept in all partition buffers
	private static final long BUFFER_FOR_PARTS = 1000000;

	// for tracking progress after processing every
	// "OUTPUT_EDGE_TRACKER_INTERVAL" edges
	private static final long OUTPUT_EDGE_TRACKER_INTERVAL = 500000;

	// the total number of edges that can be kept in each partition buffer
	private long partBufferSize;

	// table consisting of available space in each partition buffer
	private long[] partBufferFreespace;

	// the number of writes to disk for creating the partition files
	private int partitionDiskWriteCount;

	private DataOutputStream[] partOutStrms;
	private PrintWriter[] partDegOutStrms;

	private long[][] edgeDestCount;

	// Each partition buffer consists of an adjacency list.
	// HashMap<srcVertexID, <[destVertexID1,edgeValue1],
	// [destVertexID2,edgeValue2]..........>[]
	private HashMap<Integer, ArrayList<int[]>>[] partBuffers;

	/**
	 * Constructor
	 * 
	 * @param baseFilename
	 * @param numParts
	 * @throws IOException
	 */
	public Preprocessor(String baseFilename, int numParts) throws IOException {
		// get the grammar info
//		GrammarChecker.loadGrammars(new File(baseFilename + ".grammar"));

		logger.info("Initializing partition generator program... ");

		this.baseFilename = baseFilename;
		
		//this is the request numParts;
		this.numParts=GlobalParams.getNumParts();
		
//		this.numParts = numParts;

//		// long[][] edgeDestCount = new long[numParts][numParts];
//		this.edgeDestCount = new long[numParts][numParts];
//
//		// initialize streams for partition files (these streams will
//		// be later filled in by sendBufferEdgestoDisk_ByteFmt())
//		partOutStrms = new DataOutputStream[numParts];
//		for (int i = 0; i < numParts; i++) {
//			partOutStrms[i] = new DataOutputStream(
//					new BufferedOutputStream(new FileOutputStream(baseFilename + ".partition." + i, true)));
//		}
//
//		// initialize streams for partition degree files (these streams will
//		// be later filled in by generatePartDegs())
//		partDegOutStrms = new PrintWriter[numParts];
//		for (int i = 0; i < numParts; i++) {
//			partDegOutStrms[i] = new PrintWriter(
//					new BufferedWriter(new FileWriter(baseFilename + ".partition." + i + ".degrees", true)));
//		}
//		logger.info("Done");
	}
	
	public void run() throws FileNotFoundException, IOException{
		
		// generate degrees file
		long degGenStartTime = System.nanoTime();
		generateGraphDegs(new FileInputStream(new File(GlobalParams.getBasefilename())));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		logger.info("Total time to create degrees file (nanoseconds): " + degGenDuration);
		
		// creating the partitions
		long creatingPartsStartTime = System.nanoTime();
		createPartVIntervals();
		
		this.edgeDestCount = new long[numParts][numParts];

		// initialize streams for partition files (these streams will
		// be later filled in by sendBufferEdgestoDisk_ByteFmt())
		partOutStrms = new DataOutputStream[numParts];
		for (int i = 0; i < numParts; i++) {
			partOutStrms[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(baseFilename + ".partition." + i, true)));
		}

		// initialize streams for partition degree files (these streams will
		// be later filled in by generatePartDegs())
		partDegOutStrms = new PrintWriter[numParts];
		for (int i = 0; i < numParts; i++) {
			partDegOutStrms[i] = new PrintWriter(
					new BufferedWriter(new FileWriter(baseFilename + ".partition." + i + ".degrees", true)));
		}
		logger.info("Done");
		
		writePartitionEdgestoFiles(new FileInputStream(new File(GlobalParams.getBasefilename())));
		
		generatePartDegs();
		long creatingPartsDuration = System.nanoTime() - creatingPartsStartTime;
		logger.info("Total time to create partitions (nanoseconds):" + creatingPartsDuration);
		
		GlobalParams.setNumParts(this.numParts);
		
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
//			if (!ln.startsWith("#")) {
				tok = ln.split("\t");
				try {
					// use + 1 if graph vertex no. starts from 0
					if (GlobalParams.getFirstVertexID() == 0) {
						src = Integer.parseInt(tok[0]) + 1;
					} 
					else if (GlobalParams.getFirstVertexID() == 1) {
						src = Integer.parseInt(tok[0]);
					}
					
					if (!outDegs.containsKey(src)) {
						outDegs.put(src, 1);
					} else {
						outDegs.put(src, outDegs.get(src) + 1);
					}
					numEdges++;
				} catch (Exception e) {
					logger.info("ERROR: " + e + "at line # " + lineCount + " : " + ln);
				}
//			}
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
		logger.info("Total number of vertices in input graph: " + this.outDegs.size());
//		logger.info(outDegs+"");
	}

	/**
	 * Allocates vertices to partitions.
	 * 
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 */
	public void createPartVIntervals() throws FileNotFoundException, UnsupportedEncodingException {

		logger.info("Allocating vertices to partitions (creating partition allocation table)...");

		// average of edges by no. of partitions
		long avgEdgesPerPartition = numEdges / numParts;

		// the heuristic for interval max
		long intervalMaxSize = (long) (avgEdgesPerPartition * 0.9);
		logger.info("Calculated partition size threshold: " + intervalMaxSize + " edges");

		// marker of the max vertex (based on Id) of the interval
		int intervalMaxVId = 0;

		// counter of the number of edges in the interval
		int intervalEdgeCount = 0;

		
		List<Integer> intervalIDs = new ArrayList<Integer>();
		List<Integer> intervalECounts = new ArrayList<Integer>();
		
		// marker of the current partition table
//		int partTabIdx = 0;


//		long totalEdgeCount = 0;
		Iterator<Entry<Integer, Integer>> it = outDegs.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> pair = it.next();
			intervalMaxVId = pair.getKey();
			intervalEdgeCount += pair.getValue();

			// w total degree > intervalMax,
			// assign the partition_interval_head to the current_Scanned_Vertex
			if (intervalEdgeCount > intervalMaxSize) {
//				partAllocTable[partTabIdx][0] = partTabIdx;
//				partAllocTable[partTabIdx][1] = intervalMaxVId;
//				partSizes[partTabIdx][1] = intervalEdgeCount;
				intervalIDs.add(intervalMaxVId);
				intervalECounts.add(intervalEdgeCount);
				
				intervalEdgeCount = 0;
			}
			else if(intervalMaxVId == outDegs.lastKey()){
				intervalIDs.add(intervalMaxVId);
				intervalECounts.add(intervalEdgeCount);
				
				intervalEdgeCount = 0;
				
				break;
			}

//			// when last partition is reached, assign partition_interval_head to
//			// last_Vertex
//			else if (isLastPartition(partTabIdx)) {
//				intervalMaxVId = outDegs.lastKey();
//				partAllocTable[partTabIdx][0] = partTabIdx;
//				partAllocTable[partTabIdx][1] = intervalMaxVId;
//				partSizes[partTabIdx][1] = numEdges - totalEdgeCount;
//				break;
//			}
		}

		logger.info("Saving partition allocation table file " + baseFilename + ".partAllocTable... ");
		PrintWriter partAllocTableOutStrm = new PrintWriter(baseFilename + ".partAllocTable", "UTF-8");
		
		// creating the Partition Allocation Table
		int[][] partAllocTable = new int[intervalIDs.size()][2];
		long partSizes[][] = new long[intervalECounts.size()][2];
		
		this.numParts = intervalIDs.size();
		assert(partAllocTable.length == partSizes.length);
		
		for(int i = 0; i < intervalIDs.size(); i++){
			partAllocTable[i][0] = i;
			partAllocTable[i][1] = intervalIDs.get(i);
			
			partSizes[i][0] = i;
			partSizes[i][1] = intervalECounts.get(i);
		}
		for (int i = 0; i < partAllocTable.length; i++) {
			partAllocTableOutStrm.println(partAllocTable[i][0] + "\t" + partAllocTable[i][1]);
		}
		logger.info("Done");
		partAllocTableOutStrm.close();

		SchedulerInfo.setPartSizes(partSizes);
		
		AllPartitions.setPartAllocTab(partAllocTable);
		
	}

	/**
	 * Generates separate out degrees files for each partition
	 * 
	 * @throws IOException
	 */
	public void generatePartDegs() throws IOException {

		logger.info("Generating degrees file for each partition... ");

		Iterator<Entry<Integer, Integer>> it = outDegs.entrySet().iterator();
		int partId = 0;
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> pair = it.next();
			partId = PartitionQuerier.findPartition(pair.getKey());
			partDegOutStrms[partId].println(pair.getKey() + "\t" + pair.getValue());
		}

		// close all streams
		for (int i = 0; i < partDegOutStrms.length; i++) {
			partDegOutStrms[i].close();
		}

		logger.info("Done");

		outDegs.clear();

	}

	/**
	 * Scans the input graph, pools edges for each partition (according to
	 * partAllocTable), and writes the edges to the corresponding partition
	 * files
	 * 
	 * @param inputStream
	 * @throws IOException
	 */
	public void writePartitionEdgestoFiles(InputStream inputStream) throws IOException {
		logger.info("Generating partition files...");

		// initialize partition buffers
		HashMap<Integer, ArrayList<int[]>>[] partitionBuffers = new HashMap[numParts];

		logger.info("Initializing partition buffers (Total buffer size = " + BUFFER_FOR_PARTS + " edges for " + numParts
				+ " partitions)... ");
		long partitionBufferSize = BUFFER_FOR_PARTS / numParts;
		long partitionBufferFreespace[] = new long[numParts];
		for (int i = 0; i < numParts; i++) {
			partitionBufferFreespace[i] = partitionBufferSize;
			partitionBuffers[i] = new HashMap<Integer, ArrayList<int[]>>();
		}
		this.partBuffers = partitionBuffers;
		this.partBufferSize = partitionBufferSize;
		this.partBufferFreespace = partitionBufferFreespace;
		logger.info("Done");

		// read the input graph edge-wise and process each edge
		logger.info("Performing second scan on input graph...");
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long lineCount = 0;
		double percentComplete = 0;
		String[] tok;
		int src = -1, dst = -1;
		byte eval = -1;
		logger.info("The total number of edges in graph: " + numEdges);
		while ((ln = ins.readLine()) != null) {
			lineCount++;
			if (lineCount % OUTPUT_EDGE_TRACKER_INTERVAL == 0) {
				percentComplete = ((double) lineCount / numEdges) * 100;
				logger.info("Reading edges to buffer from disk. Reading line #"
						+ NumberFormat.getNumberInstance(Locale.US).format(lineCount) + "("
						+ (double) Math.round(percentComplete * 100) / 100 + "%)...");
			}
//			if (!ln.startsWith("#")) {
				try {
					tok = ln.split("\t");
					// Edge list: <src> <dst> <value>

					// use + 1 if graph vertex no. starts from 0
					if (GlobalParams.getFirstVertexID() == 0) {
						src = Integer.parseInt(tok[0]) + 1;
						dst = Integer.parseInt(tok[1]) + 1;
					}
					if (GlobalParams.getFirstVertexID() == 1) {
						src = Integer.parseInt(tok[0]);
						dst = Integer.parseInt(tok[1]);
					}

					// ignore edge-value if dataflow
					if (GlobalParams.getInputGraphType().compareTo("DATAFLOW") == 0) {
						eval = 0;
					}
					if (GlobalParams.getInputGraphType().compareTo("POINTSTO") == 0) {
						// if (tok[2].compareTo("D") == 0) {
						// eval = 9;
						// }
						// if (tok[2].compareTo("A") == 0) {
						// eval = 11;
						// }
						// eval = Integer.parseInt(tok[2]);
						eval = GrammarChecker.getValue(tok[2].trim());
					}

					assert (src != -1 && dst != -1 && eval != -1);
					
					incrementEdgeDestCount(src, dst);
					
					addEdgetoBuffer(src, dst, eval);

				} catch (Exception e) {
					logger.info("ERROR: " + e + "at line # " + lineCount + " : " + ln);
				}
//			}
		}

		// TEST PRINT edgedestcounts
		// for (int i = 0; i < numParts; i++) {
		// System.out.println("Destination Edge counts of partition " + i);
		// for (int j = 0; j < numParts; j++) {
		// System.out.print(this.edgeDestCount[i][j] + " ");
		// }
		// System.out.println();
		// }

		// write edge dest counts to file
		logger.info("Saving edge destination counts to file... ");
		saveEDC();
		logger.info("Done");

		// write part edge sizes to file
		logger.info("Saving partition sizes to file... ");
		savePartSizes();
		logger.info("Done");

		SchedulerInfo.setEdgeDestCount(edgeDestCount);

		// send any remaining edges in the buffer to disk
		for (int i = 0; i < numParts; i++) {
			sendBufferEdgestoDisk_ByteFmt(i);
		}

		// close all streams
		for (int i = 0; i < partOutStrms.length; i++) {
			partOutStrms[i].close();
		}

		logger.info("Partition files created.");
		logger.info("Total # writes to disk for creating partition files: " + partitionDiskWriteCount);

	}

	/**
	 * Checks each edge, finds the appropriate partition, adds them to the
	 * corresponding partition buffer, and then CALLS
	 * sendBufferEdgestoDisk_NmlFmt/ByteFmt() when the buffer is full (CALLED BY
	 * writePartitionEdgestoFiles())
	 * 
	 * @param srcVId
	 * @param destVId
	 * @param edgeValue
	 * @throws IOException
	 */
	private void addEdgetoBuffer(int srcVId, int destVId, int edgeValue) throws IOException {

		int partitionId = PartitionQuerier.findPartition(srcVId);

		// get the adjacencyList from the relevant partition buffer
		HashMap<Integer, ArrayList<int[]>> vertexAdjList = partBuffers[partitionId];

		// store the (destVId, edgeValue) pair in an array
		int[] destEdgeValPair = new int[2];
		destEdgeValPair[0] = destVId;
		destEdgeValPair[1] = edgeValue;

		/*
		 * IF the srcVId row already exists, save the (destVId, edgeValue) pair
		 * array in the vertexAdjList for the given SrcVId. ELSE create a new
		 * arrayList for the SrcVId and then add to the vertexAdjList of this
		 * partition buffer
		 */
		if (vertexAdjList.containsKey(srcVId)) {
			vertexAdjList.get(srcVId).add(destEdgeValPair);
		} else {
			ArrayList<int[]> srcVIdRow = new ArrayList<int[]>();
			srcVIdRow.add(destEdgeValPair);
			vertexAdjList.put(srcVId, srcVIdRow);
		}

		partBufferFreespace[partitionId]--;

		// if partition buffer is full transfer the partition buffer to file
		if (isPartitionBufferFull(partitionId)) {
			// logger.info("Partition buffer is full for partition id "
			// + partitionId
			// + ", writing the edges for this buffer to disk.");
			// System.out.print("Partition buffer for partition # " +
			// partitionId + " full, writing to disk... ");
			sendBufferEdgestoDisk_ByteFmt(partitionId);
			// System.out.print("Done\n");
		}
	}

	/**
	 * Transfers the buffer edges to disk (performs the actual write operation),
	 * storing them in Byte Format (CALLED BY addEdgetoBuffer() when buffer is
	 * full and CALLED BY writePartitionEdgestoFiles() when there are no more
	 * edges to be read from input graph)
	 * 
	 * @param partitionId
	 * @throws IOException
	 */
	private void sendBufferEdgestoDisk_ByteFmt(int partitionId) throws IOException {
		partitionDiskWriteCount++;

		DataOutputStream adjListOutputStream = partOutStrms[partitionId];

		int srcVId, destVId, count;
		int edgeValue;

		// get the adjacencyList from the relevant partition buffer
		HashMap<Integer, ArrayList<int[]>> vertexAdjList = partBuffers[partitionId];
		Iterator<Map.Entry<Integer, ArrayList<int[]>>> it = vertexAdjList.entrySet().iterator();

		/*
		 * write the vertex adjacency lists in the file (srcVId:4 bytes,count:4
		 * bytes,destVId:4 bytes,edgeValue:1 byte)
		 */
		while (it.hasNext()) {
			Map.Entry<Integer, ArrayList<int[]>> pair = it.next();

			// write the srcId
			srcVId = pair.getKey();
			adjListOutputStream.writeInt(srcVId);
			// logger.info("src="+srcVId);

			// get the relevant srcVIdrow row from the adjacencyList
			ArrayList<int[]> srcVIdRow = pair.getValue();

			// write the count
			count = srcVIdRow.size();
			adjListOutputStream.writeInt(count);
			// logger.info("count="+count);

			// write the destId edgeValue pair
			for (int i = 0; i < srcVIdRow.size(); i++) {
				int[] destIdEdgeVal = srcVIdRow.get(i);
				destVId = destIdEdgeVal[0];
				edgeValue = destIdEdgeVal[1];
				adjListOutputStream.writeInt(destVId);
				adjListOutputStream.writeByte((byte) edgeValue);
			}
		}

		// empty the buffer
		vertexAdjList.clear();
		partBufferFreespace[partitionId] = partBufferSize;
	}

	/**
	 * Transfers the buffer edges to disk (performs the actual write operation)
	 * storing them in Normal Format. This method is called by addEdgetoBuffer()
	 * when buffer is full and called by writePartitionEdgestoFiles() when there
	 * are no more edges to be read from input graph.
	 * 
	 * @param partitionId
	 * @throws IOException
	 */
//	@SuppressWarnings("unused")
//	private void sendBufferEdgestoDisk_NmlFmt(int partitionId) throws IOException {
//		int srcVId, destVId, count;
//		byte edgeValue;
//
//		PrintWriter adjListOutputStream = new PrintWriter(
//				new BufferedWriter(new FileWriter(baseFilename + ".partition." + partitionId, true)));
//
//		// get the adjacencyList from the relevant partition buffer
//		HashMap<Integer, ArrayList<int[]>> vertexAdjList = partBuffers[partitionId];
//		Iterator<Map.Entry<Integer, ArrayList<int[]>>> it = vertexAdjList.entrySet().iterator();
//
//		/*
//		 * Write the adjacencyList in the disk in the following format:
//		 * <SrcId-1><Count><DestId-1><EdgeVal-1><DestId-2><EdgeVal-2>........<
//		 * SrcId-n><Count><DestId-1><EdgeVal-1><DestId-2><EdgeVal-2>
//		 */
//		while (it.hasNext()) {
//
//			Map.Entry<Integer, ArrayList<int[]>> pair = it.next();
//
//			// write the srcId
//			srcVId = pair.getKey();
//			adjListOutputStream.println(srcVId);
//
//			// get the relevant srcVIdrow row from the adjacencyList
//			ArrayList<int[]> srcVIdRow = pair.getValue();
//
//			// write the count
//			count = srcVIdRow.size();
//			adjListOutputStream.println(count);
//
//			// write the destId edgeValue pair
//			for (int i = 0; i < srcVIdRow.size(); i++) {
//				int[] destIdEdgeVal = srcVIdRow.get(i);
//				destVId = destIdEdgeVal[0];
//				edgeValue = (byte) destIdEdgeVal[1];
//				adjListOutputStream.println(destVId);
//				adjListOutputStream.println(edgeValue);
//			}
//		}
//		adjListOutputStream.close();
//
//		// empty the buffer
//		vertexAdjList.clear();
//		partBufferFreespace[partitionId] = partBufferSize;
//	}

	/**
	 * Stores the counts of the destination partitions for each edge in each
	 * partition in the memory.
	 * 
	 * @param src
	 * @param dest
	 */
	private void incrementEdgeDestCount(int src, int dest) {
		// test edges for a partition pair
		// if (PartitionQuerier.findPartition(src) == 0 &
		// PartitionQuerier.findPartition(dest) == 0) {
		// System.out.println(src + " " + dest);
		// }

		if (PartitionQuerier.findPartition(dest) != -1) {
			this.edgeDestCount[PartitionQuerier.findPartition(src)][PartitionQuerier.findPartition(dest)]++;
		}
	}

	/**
	 * Stores the counts of the destination partitions for each edge in each
	 * partition in disk.
	 * 
	 * @throws IOException
	 */
	private void saveEDC() throws IOException {
		PrintWriter edgeDestCountsOutStrm = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".edgeDestCounts", true)));
		for (int i = 0; i < numParts; i++) {
			for (int j = 0; j < numParts; j++) {
				edgeDestCountsOutStrm.println(i + "\t" + j + "\t" + edgeDestCount[i][j]);
			}
		}
		edgeDestCountsOutStrm.close();

	}

	/**
	 * Stores the counts of partition sizes of each partition in disk.
	 * 
	 * @throws IOException
	 */
	private void savePartSizes() throws IOException {
		PrintWriter partSizesOutStrm = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".partSizes", true)));
		long[][] partSizes = SchedulerInfo.getPartSizes();
		for (int i = 0; i < numParts; i++) {
			partSizesOutStrm.println(partSizes[i][1]);
		}
		partSizesOutStrm.close();
	}

//	/**
//	 * Checks whether partition indicated by partTabId is the last partition.
//	 * 
//	 * @param partTabIdx
//	 * @return
//	 */
//	private boolean isLastPartition(int partTabIdx) {
//		return (partTabIdx == (numParts - 1) ? true : false);
//	}

	/**
	 * Checks whether partition buffer is full. This method is called by
	 * addEdgetoBuffer() method.
	 * 
	 * @param partitionId
	 * @return
	 */
	private boolean isPartitionBufferFull(int partitionId) {
		return (partBufferFreespace[partitionId] == 0 ? true : false);
	}

}