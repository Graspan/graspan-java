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

/**
 * 
 * @author Aftab
 *
 */
public class PartitionGenerator {

	/**
	 * numPartitions:number of input partitions|numEdges:total number of input
	 * edges in the graph|baseFilename:path of the input graph
	 * file|outDegreesMap:map of vertex and degrees|BUFFER_FOR_PARTITIONS:the
	 * total number of edges that can be kept in all partition buffers|
	 * partitionBufferSize:the total number of edges that can be kept in each
	 * partition buffer|partitionBufferFreespace:table consisting of available
	 * space in each partition buffer|partitionDiskWriteCount:the number of
	 * writes to disk for creating the partition files
	 */
	private int numPartitions;
	private long numEdges;
	private String baseFilename;
	private TreeMap<Integer, Integer> outDegreesMap;
	public static int[] partAllocTable;
	private static final long BUFFER_FOR_PARTITIONS = 100000000;
	private long partitionBufferSize;
	private long[] partitionBufferFreespace;
	private DataOutputStream[] partitionOutputStreams;
	private int partitionDiskWriteCount;

	// Each partition buffer consists of an adjacency list.
	// HashMap<srcVertexID, <[destVertexID1,edgeValue1],
	// [destVertexID2,edgeValue2]..........>[]
	private HashMap<Integer, ArrayList<Integer[]>>[] partitionBuffers;

	/**
	 * Constructor
	 * 
	 * @param baseFilename
	 * @param numPartitions
	 * @throws IOException
	 */
	public PartitionGenerator(String baseFilename, int numPartitions) throws IOException {
		System.out.print("Initializing partition generator program... ");

		this.numPartitions = numPartitions;
		this.baseFilename = baseFilename;

		// initialize partition allocation table (it stores the maximum src
		// vertex id for each partition)
		partAllocTable = new int[numPartitions];

		// create the streams for the empty partition files (these streams will
		// be later filled in by sendBufferEdgestoDisk_ByteFmt())
		partitionOutputStreams = new DataOutputStream[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			this.partitionOutputStreams[i] = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(baseFilename + ".partition." + i, true)));
		}
		System.out.print("Done\n");
	}

	/**
	 * Scans the entire graph and counts the out-degrees of all the vertices.
	 * This is the Preliminary Scan TODO need to store the degrees file in a
	 * more space-efficient way
	 * 
	 * @param inputStream
	 * @param format
	 * @throws IOException
	 */
	public void generateDegrees(InputStream inputStream) throws IOException {
		System.out.println("Generating degrees file");
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long numEdges = 0;
		TreeMap<Integer, Integer> outDegreesMap = new TreeMap<Integer, Integer>();
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
				if (!outDegreesMap.containsKey(src)) {
					outDegreesMap.put(src, 1);
				} else {
					outDegreesMap.put(src, outDegreesMap.get(src) + 1);
				}
				numEdges++;
			}
		}
		System.out.println("\nDone");

		this.numEdges = numEdges;
		System.out.print(">Total number of edges in input graph: " + numEdges + "\n");

		// Save the degrees on disk
		System.out.print("Saving degrees file " + baseFilename + ".degrees... ");
		Iterator it = outDegreesMap.entrySet().iterator();

		PrintWriter outDegOutputStream = new PrintWriter(baseFilename + ".degrees", "UTF-8");
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			outDegOutputStream.println(pair.getKey() + "\t" + pair.getValue());
		}
		outDegOutputStream.close();
		this.outDegreesMap = outDegreesMap;

		/*
		 * ERROR when using the following code instead of the one above.:
		 * Exception in thread "main" java.lang.ClassCastException:
		 * java.util.Collections$UnmodifiableMap cannot be cast to
		 * java.util.TreeMap at
		 * edu.uci.ics.cs.gdtc.PartitionGenerator.generateDegrees(
		 * PartitionGenerator.java:94) at
		 * edu.uci.ics.cs.gdtc.MainPreprocessor.main(MainGraphDTC. java:24)
		 */
		// this.outDegreesMap = (TreeMap<Integer, Integer>)
		// Collections.unmodifiableMap(degreesMap);
		System.out.println("Done");
	}

	/**
	 * Allocates vertex intervals to partitions.
	 * 
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 */
	public void allocateVIntervalstoPartitions() throws FileNotFoundException, UnsupportedEncodingException {
		System.out.print("Allocating vertices to partitions (creating partition allocation table)\n");

		// average of edges by no. of partitions
		long avgEdgesPerPartition = Math.floorDiv(numEdges, numPartitions);

		// the heuristic for interval max
		long intervalMax = (long) (avgEdgesPerPartition * 0.9);
		System.out.println(">Calculated partition size threshold: " + intervalMax + " edges");

		// marker of the max vertex (based on Id) of the interval
		int intervalHeadVertexId = 0;

		// counter of the number of edges in the interval
		int intervalEdgeCount = 0;

		// marker of the current partition table
		int partTabIdx = 0;

		Iterator it = outDegreesMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			intervalHeadVertexId = (Integer) pair.getKey();
			intervalEdgeCount += (Integer) pair.getValue();

			// w total degree > intervalMax,
			// assign the partition_interval_head to the current_Scanned_Vertex
			if (intervalEdgeCount > intervalMax & !isLastPartition(partTabIdx)) {
				partAllocTable[partTabIdx] = intervalHeadVertexId;
				intervalEdgeCount = 0;
				partTabIdx++;
			}

			// when last partition is reached, assign partition_interval_head to
			// last_Vertex
			else if (isLastPartition(partTabIdx)) {
				intervalHeadVertexId = outDegreesMap.lastKey();
				partAllocTable[partTabIdx] = intervalHeadVertexId;
				break;
			}
		}

		System.out.print("Saving partition allocation table file " + baseFilename + ".partAllocTable\n");
		PrintWriter partAllocTableOutputStream = new PrintWriter(baseFilename + ".partAllocTable", "UTF-8");
		for (int i = 0; i < partAllocTable.length; i++) {
			partAllocTableOutputStream.println(partAllocTable[i]);
		}
		partAllocTableOutputStream.close();

	}

	/**
	 * check whether we have reached the last partition (called by
	 * allocateVIntervalstoPartitions() method)
	 * 
	 * @param partTabIdx
	 * @return
	 */
	private boolean isLastPartition(int partTabIdx) {
		return (partTabIdx == (partAllocTable.length - 1) ? true : false);
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
		System.out.println("Generating partition files");

		// initialize partition buffers
		HashMap<Integer, ArrayList<Integer[]>>[] partitionBuffers = new HashMap[numPartitions];

		System.out.print("Initializing partition buffers (Total buffer size = " + BUFFER_FOR_PARTITIONS + " edges for "
				+ numPartitions + " partitions)... ");
		long partitionBufferSize = Math.floorDiv(BUFFER_FOR_PARTITIONS, numPartitions);
		long partitionBufferFreespace[] = new long[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			partitionBufferFreespace[i] = partitionBufferSize;
			HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = new HashMap<Integer, ArrayList<Integer[]>>();
			partitionBuffers[i] = new HashMap<Integer, ArrayList<Integer[]>>();
		}
		this.partitionBuffers = partitionBuffers;
		this.partitionBufferSize = partitionBufferSize;
		this.partitionBufferFreespace = partitionBufferFreespace;
		System.out.print("Done\n");

		// read the input graph edge-wise and process each edge
		System.out.println("Performing second scan on input graph");
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long lineCount = 0;
		while ((ln = ins.readLine()) != null) {
			lineCount++;
			if (lineCount % 30000000 == 0) {
				double percentComplete = (lineCount / numEdges) * 100;
				System.out.print("Sending edges to buffer, reading line "
						+ NumberFormat.getNumberInstance(Locale.US).format(lineCount) + "(" + percentComplete + "%)");
			}
			if (!ln.startsWith("#")) {
				String[] tok = ln.split("\t");
				// Edge list: <src> <dst> <value>
				addEdgetoBuffer(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), Integer.parseInt(tok[2]));
			}
		}

		// send any remaining edges in the buffer to disk
		for (int i = 0; i < numPartitions; i++) {
			sendBufferEdgestoDisk_ByteFmt(i);
		}

		// close all streams
		for (int i = 0; i < this.partitionOutputStreams.length; i++) {
			this.partitionOutputStreams[i].close();
		}

		System.out.println("\nPartition files created");
		System.out.println(
				">Total number of writes to disk for creating partition files: " + this.partitionDiskWriteCount);

		this.outDegreesMap.clear();
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

		int partitionId = findPartition(srcVId);

		// get the adjacencyList from the relevant partition buffer
		HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partitionBuffers[partitionId];

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

		partitionBufferFreespace[partitionId]--;

		// if partition buffer is full transfer the partition buffer to file
		if (isPartitionBufferFull(partitionId)) {
			// System.out.print("Partition buffer for partition # " +
			// partitionId + " full, writing to disk... ");
			sendBufferEdgestoDisk_ByteFmt(partitionId);
			// System.out.print("Done\n");
		}
	}

	/**
	 * searches the partAllocTable to find out which partition a vertex belongs
	 * (CALLED BY addEdgetoBuffer() method)
	 * 
	 * @param srcV
	 */
	private int findPartition(int srcVId) {
		int partitionId = -1;
		for (int i = 0; i < numPartitions; i++) {
			if (srcVId <= partAllocTable[i]) {
				partitionId = i;
				break;
			}
		}
		return partitionId;
	}

	/**
	 * Checks whether partition buffer is full (CALLED BY addEdgetoBuffer()
	 * method)
	 * 
	 * @param partitionId
	 * @return
	 */
	private boolean isPartitionBufferFull(int partitionId) {
		return (partitionBufferFreespace[partitionId] == 0 ? true : false);
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

		DataOutputStream adjListOutputStream = this.partitionOutputStreams[partitionId];

		int srcVId, destVId, count;
		int edgeValue;

		// get the adjacencyList from the relevant partition buffer
		HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partitionBuffers[partitionId];
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
		partitionBufferFreespace[partitionId] = partitionBufferSize;
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
		HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partitionBuffers[partitionId];
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
		partitionBufferFreespace[partitionId] = partitionBufferSize;
	}

}
