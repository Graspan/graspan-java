package edu.uci.ics.cs.gdtc.preproc;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
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
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import edu.uci.ics.cs.gdtc.data.AllPartitions;
import edu.uci.ics.cs.gdtc.data.PartitionQuerier;
import edu.uci.ics.cs.gdtc.data.SchedulerInfo;

/**
 * 
 * @author Aftab
 *
 */
public class PartitionGenerator {

	/**
	 * numParts:number of input partitions|numEdges:total number of input edges
	 * in the graph|baseFilename:path of the input graph file|outDegs:map of
	 * vertex and degrees|BUFFER_FOR_PARTS:the total number of edges that can be
	 * kept in all partition buffers| partBufferSize:the total number of edges
	 * that can be kept in each partition buffer|partBufferFreespace:table
	 * consisting of available space in each partition
	 * buffer|partitionDiskWriteCount:the number of writes to disk for creating
	 * the partition files
	 */
	private int numParts;
	private long numEdges;
	private String baseFilename;
	private TreeMap<Integer, Integer> outDegs;
	private static final long BUFFER_FOR_PARTS = 100000000;
	private long partBufferSize;
	private long[] partBufferFreespace;
	private DataOutputStream[] partOutStrms;
	private PrintWriter[] partDegOutStrms;
	private int partitionDiskWriteCount;
	private long[][] edgeDestCount;

	// Each partition buffer consists of an adjacency list.
	// HashMap<srcVertexID, <[destVertexID1,edgeValue1],
	// [destVertexID2,edgeValue2]..........>[]
	private HashMap<Integer, ArrayList<Integer[]>>[] partBuffers;

	/**
	 * Constructor
	 * 
	 * @param baseFilename
	 * @param numParts
	 * @throws IOException
	 */
	public PartitionGenerator(String baseFilename, int numParts) throws IOException {
		System.out.print("Initializing partition generator program... ");

		this.numParts = numParts;
		this.baseFilename = baseFilename;

		long[][] edgeDestCount = new long[numParts][numParts];
		this.edgeDestCount = edgeDestCount;

		// initialize streams for partition files (these streams will
		// be later filled in by sendBufferEdgestoDisk_ByteFmt())
		partOutStrms = new DataOutputStream[numParts];
		for (int i = 0; i < numParts; i++) {
			partOutStrms[i] = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(baseFilename + ".partition." + i, true)));
		}

		// initialize streams for partition degree files (these streams will
		// be later filled in by generatePartDegs())
		partDegOutStrms = new PrintWriter[numParts];
		for (int i = 0; i < numParts; i++) {
			partDegOutStrms[i] = new PrintWriter(
					new BufferedWriter(new FileWriter(baseFilename + ".partition." + i + ".degrees", true)));
		}
		System.out.print("Done\n");
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
		System.out.println("Generating degrees file...");
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long numEdges = 0;
		TreeMap<Integer, Integer> outDegs = new TreeMap<Integer, Integer>();
		// read inputgraph line-by-line and keep incrementing degree
		System.out.print("Performing first scan on input graph... ");
		long lineCount = 0;
		long readStartTime = System.nanoTime();
		while ((ln = ins.readLine()) != null) {
			lineCount++;
			if (lineCount % 30000000 == 0) {
				System.out
						.print("\nReading edge #" + NumberFormat.getNumberInstance(Locale.US).format(lineCount) + ".");
				double readSpeed = 30000000 / ((System.nanoTime() - readStartTime) / 1000000000);
				System.out.print(" Read speed: " + readSpeed + " edges/sec");
				readStartTime = System.nanoTime();
			}
			if (!ln.startsWith("#")) {
				String[] tok = ln.split("\t");
				int src = Integer.parseInt(tok[0]);
				if (!outDegs.containsKey(src)) {
					outDegs.put(src, 1);
				} else {
					outDegs.put(src, outDegs.get(src) + 1);
				}
				numEdges++;
			}
		}
		System.out.println("\nDone");

		this.numEdges = numEdges;
		System.out.print(">Total number of edges in input graph: " + numEdges + "\n");

		// Save the degrees on disk
		System.out.print("Saving degrees file " + baseFilename + ".degrees... ");
		Iterator it = outDegs.entrySet().iterator();

		PrintWriter outDegOutStrm = new PrintWriter(baseFilename + ".degrees", "UTF-8");
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			outDegOutStrm.println(pair.getKey() + "\t" + pair.getValue());
		}
		outDegOutStrm.close();
		this.outDegs = outDegs;

		System.out.println("Done");
	}

	/**
	 * Allocates vertices to partitions.
	 * 
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 */
	public void createPartVIntervals() throws FileNotFoundException, UnsupportedEncodingException {

		System.out.print("Allocating vertices to partitions (creating partition allocation table)...\n");

		// average of edges by no. of partitions
		long avgEdgesPerPartition = Math.floorDiv(numEdges, numParts);

		// the heuristic for interval max
		long intervalMaxSize = (long) (avgEdgesPerPartition * 0.9);
		System.out.println(">Calculated partition size threshold: " + intervalMaxSize + " edges");

		// marker of the max vertex (based on Id) of the interval
		int intervalMaxVId = 0;

		// counter of the number of edges in the interval
		int intervalEdgeCount = 0;

		// marker of the current partition table
		int partTabIdx = 0;

		Iterator it = outDegs.entrySet().iterator();

		// creating the Partition Allocation Table
		int[] partAllocTable = new int[numParts];

		long partSizes[] = new long[numParts];
		long totalEdgeCount = 0;

		while (it.hasNext()) {
			Map.Entry<Integer, Integer> pair = (Map.Entry) it.next();
			intervalMaxVId = pair.getKey();
			intervalEdgeCount += pair.getValue();

			// w total degree > intervalMax,
			// assign the partition_interval_head to the current_Scanned_Vertex
			if (intervalEdgeCount > intervalMaxSize & !isLastPartition(partTabIdx, numParts)) {
				partAllocTable[partTabIdx] = intervalMaxVId;
				partSizes[partTabIdx] = intervalEdgeCount;
				totalEdgeCount = totalEdgeCount + intervalEdgeCount;
				intervalEdgeCount = 0;
				partTabIdx++;
			}

			// when last partition is reached, assign partition_interval_head to
			// last_Vertex
			else if (isLastPartition(partTabIdx, numParts)) {
				intervalMaxVId = outDegs.lastKey();
				partAllocTable[partTabIdx] = intervalMaxVId;
				partSizes[partTabIdx] = numEdges - totalEdgeCount;
				break;
			}
		}

		System.out.print("Saving partition allocation table file " + baseFilename + ".partAllocTable... ");
		PrintWriter partAllocTableOutStrm = new PrintWriter(baseFilename + ".partAllocTable", "UTF-8");
		for (int i = 0; i < partAllocTable.length; i++) {
			partAllocTableOutStrm.println(partAllocTable[i]);
		}
		System.out.println("Done");

		SchedulerInfo.setPartSizes(partSizes);
		AllPartitions.setPartAllocTab(partAllocTable);
		partAllocTableOutStrm.close();

	}

	/**
	 * Generates separate out degrees files for each partition
	 * 
	 * @throws IOException
	 */
	public void generatePartDegs() throws IOException {

		System.out.print("Generating degrees file for each partition... ");

		Iterator it = outDegs.entrySet().iterator();
		int partId = 0;
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> pair = (Map.Entry) it.next();
			partId = PartitionQuerier.findPartition(pair.getKey());
			partDegOutStrms[partId].println(pair.getKey() + "\t" + pair.getValue());
		}

		// close all streams
		for (int i = 0; i < partDegOutStrms.length; i++) {
			partDegOutStrms[i].close();
		}

		System.out.println("Done");

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
		System.out.println("Generating partition files...");

		// initialize partition buffers
		HashMap<Integer, ArrayList<Integer[]>>[] partitionBuffers = new HashMap[numParts];

		System.out.print("Initializing partition buffers (Total buffer size = " + BUFFER_FOR_PARTS + " edges for "
				+ numParts + " partitions)... ");
		long partitionBufferSize = Math.floorDiv(BUFFER_FOR_PARTS, numParts);
		long partitionBufferFreespace[] = new long[numParts];
		for (int i = 0; i < numParts; i++) {
			partitionBufferFreespace[i] = partitionBufferSize;
			partitionBuffers[i] = new HashMap<Integer, ArrayList<Integer[]>>();
		}
		this.partBuffers = partitionBuffers;
		this.partBufferSize = partitionBufferSize;
		this.partBufferFreespace = partitionBufferFreespace;
		System.out.print("Done\n");

		// read the input graph edge-wise and process each edge
		System.out.println("Performing second scan on input graph...");
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long lineCount = 0;
		while ((ln = ins.readLine()) != null) {
			lineCount++;
			if (lineCount % 30000000 == 0) {
				double percentComplete = (lineCount / numEdges) * 100;
				System.out.println("Sending edges to buffer, reading line "
						+ NumberFormat.getNumberInstance(Locale.US).format(lineCount) + "(" + percentComplete
						+ "%)...");
			}
			if (!ln.startsWith("#")) {
				String[] tok = ln.split("\t");
				// Edge list: <src> <dst> <value>
				incrementEdgeDestCount(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]));
				addEdgetoBuffer(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), Integer.parseInt(tok[2]));
			}
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
		System.out.print("Saving edge destination counts to file...");
		writeEdgeDestCountstoFile();
		System.out.println("Done");

		// write part edge sizes to file
		System.out.print("Saving partition sizes to file...");
		writeTotalPartEdgestoFile();
		System.out.println("Done");

		SchedulerInfo.setEdgeDestCount(edgeDestCount);

		// send any remaining edges in the buffer to disk
		for (int i = 0; i < numParts; i++) {
			sendBufferEdgestoDisk_ByteFmt(i);
		}

		// close all streams
		for (int i = 0; i < partOutStrms.length; i++) {
			partOutStrms[i].close();
		}

		System.out.println("Partition files created.");
		System.out.println(">Total number of writes to disk for creating partition files: " + partitionDiskWriteCount);

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
		HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partBuffers[partitionId];

		// store the (destVId, edgeValue) pair in an array
		Integer[] destEdgeValPair = new Integer[2];
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
			ArrayList<Integer[]> srcVIdRow = new ArrayList<Integer[]>();
			srcVIdRow.add(destEdgeValPair);
			vertexAdjList.put(srcVId, srcVIdRow);
		}

		partBufferFreespace[partitionId]--;

		// if partition buffer is full transfer the partition buffer to file
		if (isPartitionBufferFull(partitionId)) {
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
		HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partBuffers[partitionId];
		Iterator<Map.Entry<Integer, ArrayList<Integer[]>>> it = vertexAdjList.entrySet().iterator();

		/*
		 * write the vertex adjacency lists in the file (srcVId:4 bytes,count:4
		 * bytes,destVId:4 bytes,edgeValue:1 byte)
		 */
		while (it.hasNext()) {
			Map.Entry<Integer, ArrayList<Integer[]>> pair = it.next();

			// write the srcId
			srcVId = pair.getKey();
			adjListOutputStream.writeInt(srcVId);

			// get the relevant srcVIdrow row from the adjacencyList
			ArrayList<Integer[]> srcVIdRow = pair.getValue();

			// write the count
			count = srcVIdRow.size();
			adjListOutputStream.writeInt(count);

			// write the destId edgeValue pair
			for (int i = 0; i < srcVIdRow.size(); i++) {
				Integer[] destIdEdgeVal = srcVIdRow.get(i);
				destVId = destIdEdgeVal[0];
				edgeValue = destIdEdgeVal[1];
				adjListOutputStream.writeInt(destVId);
				adjListOutputStream.writeByte(edgeValue);
			}
		}

		// empty the buffer
		vertexAdjList.clear();
		partBufferFreespace[partitionId] = partBufferSize;
	}

	/**
	 * Transfers the buffer edges to disk (performs the actual write operation)
	 * storing them in Normal Format (CALLED BY addEdgetoBuffer() when buffer is
	 * full and CALLED BY writePartitionEdgestoFiles() when there are no more
	 * edges to be read from input graph)
	 * 
	 * @param partitionId
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private void sendBufferEdgestoDisk_NmlFmt(int partitionId) throws IOException {
		int srcVId, destVId, count;
		byte edgeValue;

		PrintWriter adjListOutputStream = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".partition." + partitionId, true)));

		// get the adjacencyList from the relevant partition buffer
		HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partBuffers[partitionId];
		Iterator<Map.Entry<Integer, ArrayList<Integer[]>>> it = vertexAdjList.entrySet().iterator();

		/*
		 * Write the adjacencyList in the disk in the following format:
		 * <SrcId-1><Count><DestId-1><EdgeVal-1><DestId-2><EdgeVal-2>........<
		 * SrcId-n><Count><DestId-1><EdgeVal-1><DestId-2><EdgeVal-2>
		 */
		while (it.hasNext()) {

			Map.Entry<Integer, ArrayList<Integer[]>> pair = it.next();

			// write the srcId
			srcVId = pair.getKey();
			adjListOutputStream.println(srcVId);

			// get the relevant srcVIdrow row from the adjacencyList
			ArrayList<Integer[]> srcVIdRow = pair.getValue();

			// write the count
			count = srcVIdRow.size();
			adjListOutputStream.println(count);

			// write the destId edgeValue pair
			for (int i = 0; i < srcVIdRow.size(); i++) {
				Integer[] destIdEdgeVal = srcVIdRow.get(i);
				destVId = destIdEdgeVal[0];
				edgeValue = Byte.parseByte(destIdEdgeVal[1].toString());
				adjListOutputStream.println(destVId);
				adjListOutputStream.println(edgeValue);
			}
		}
		adjListOutputStream.close();

		// empty the buffer
		vertexAdjList.clear();
		partBufferFreespace[partitionId] = partBufferSize;
	}

	/**
	 * Stores the counts of the destination partitions for each edge in each
	 * partition in the memory.
	 * 
	 * @param src
	 * @param dest
	 */
	private void incrementEdgeDestCount(int src, int dest) {
		long[][] edgeDestCount = this.edgeDestCount;

		// test edges for a partition pair
		// if (PartitionQuerier.findPartition(src) == 0 &
		// PartitionQuerier.findPartition(dest) == 0) {
		// System.out.println(src + " " + dest);
		// }

		if (PartitionQuerier.findPartition(dest) != -1) {
			edgeDestCount[PartitionQuerier.findPartition(src)][PartitionQuerier.findPartition(dest)]++;
		}
	}

	/**
	 * Stores the counts of the destination partitions for each edge in each
	 * partition in disk.
	 * 
	 * @throws IOException
	 */
	private void writeEdgeDestCountstoFile() throws IOException {
		PrintWriter edgeDestCountsOutStrm = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".edgeDestCounts.", true)));
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
	private void writeTotalPartEdgestoFile() throws IOException {
		PrintWriter partSizesOutStrm = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".partSizes.", true)));
		long[] partSizes = SchedulerInfo.getPartSizes();
		for (int i = 0; i < numParts; i++) {
			partSizesOutStrm.println(partSizes[i]);
		}
		partSizesOutStrm.close();
	}

	/**
	 * check whether partition indicated by partTabId is the last partition
	 * 
	 * @param partTabIdx
	 * @return
	 */
	private static boolean isLastPartition(int partTabIdx, int numParts) {
		return (partTabIdx == (numParts - 1) ? true : false);
	}

	/**
	 * Checks whether partition buffer is full (CALLED BY addEdgetoBuffer()
	 * method)
	 * 
	 * @param partitionId
	 * @return
	 */
	private boolean isPartitionBufferFull(int partitionId) {
		return (partBufferFreespace[partitionId] == 0 ? true : false);
	}

}