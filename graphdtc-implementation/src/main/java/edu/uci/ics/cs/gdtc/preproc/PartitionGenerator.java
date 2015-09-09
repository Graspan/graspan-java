package edu.uci.ics.cs.gdtc.preproc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * @author Aftab
 *
 */
public class PartitionGenerator {

	/**
	 * numPartitions - number of input partitions numEdges - total number of
	 * input edges in the graph baseFilename - path of the input graph file
	 * outDegreesMap - map of vertex and degrees BUFFER_FOR_PARTITIONS - the
	 * total number of edges that can be kept in all partition buffers
	 * partitionBufferSize - the total number of edges that can be kept in each
	 * partition buffer partitionBufferFreespace - table consisting of available
	 * space in each partition buffer
	 */
	private int numPartitions;
	private long numEdges;
	private String baseFilename;
	private TreeMap<Integer, Integer> outDegreesMap;
	private int[] partitionAllocationTable;
	private static final int BUFFER_FOR_PARTITIONS = 10;
	private int partitionBufferSize;
	private int[] partitionBufferFreespace;

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
		this.numPartitions = numPartitions;
		this.baseFilename = baseFilename;

		// initialize partition allocation table
		partitionAllocationTable = new int[numPartitions];

	}

	/**
	 * Scans the entire graph and counts the out-degrees of all the vertices.
	 * This is the Preliminary Scan
	 * 
	 * @param inputStream
	 * @param format
	 * @throws IOException
	 */
	public void generateDegrees(InputStream inputStream) throws IOException {
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		long numEdges = 0;
		TreeMap<Integer, Integer> outDegreesMap = new TreeMap<Integer, Integer>();

		// read inputgraph line-by-line and keep incrementing degree
		while ((ln = ins.readLine()) != null) {
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
		this.numEdges = numEdges;

		// Save the degrees on disk
		Iterator it = outDegreesMap.entrySet().iterator();

		PrintWriter degreeOutputStream = new PrintWriter(baseFilename + ".degrees", "UTF-8");
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			degreeOutputStream.println(pair.getKey() + "\t" + pair.getValue());
		}
		degreeOutputStream.close();
		this.outDegreesMap = outDegreesMap;

		/*
		 * ERROR when using the following code instead of the one above.:
		 * Exception in thread "main" java.lang.ClassCastException:
		 * java.util.Collections$UnmodifiableMap cannot be cast to
		 * java.util.TreeMap at
		 * edu.uci.ics.cs.gdtc.preproc.PartitionGenerator.generateDegrees(
		 * PartitionGenerator.java:94) at
		 * edu.uci.ics.cs.gdtc.preproc.MainPreprocessor.main(MainPreprocessor.
		 * java:24)
		 */
		// this.outDegreesMap = (TreeMap<Integer, Integer>)
		// Collections.unmodifiableMap(degreesMap);
	}

	/**
	 * Allocates vertex intervals to partitions.
	 */
	public void allocateVIntervalstoPartitions() {

		// average of edges by no. of partitions
		long avgEdgesPerPartition = Math.floorDiv(numEdges, numPartitions);

		// the heuristic for interval max
		long intervalMax = (long) (avgEdgesPerPartition * 0.9);

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
				partitionAllocationTable[partTabIdx] = intervalHeadVertexId;
				intervalEdgeCount = 0;
				partTabIdx++;
			}

			// when last partition is reached, assign partition_interval_head to
			// last_Vertex
			else if (isLastPartition(partTabIdx)) {
				intervalHeadVertexId = outDegreesMap.lastKey();
				partitionAllocationTable[partTabIdx] = intervalHeadVertexId;
				break;
			}
		}
	}

	/**
	 * check whether we have reached the last partition (called by
	 * allocateVIntervalstoPartitions() method)
	 * 
	 * @param partTabIdx
	 * @return
	 */
	private boolean isLastPartition(int partTabIdx) {
		return (partTabIdx == (partitionAllocationTable.length - 1) ? true : false);
	}

	/**
	 * Scans the input graph, pools edges for each partition (according to
	 * partitionAllocationTable), and writes the edges to the corresponding
	 * partition files
	 * 
	 * @param inputStream
	 * @throws IOException
	 */
	public void writePartitionEdgestoFiles(InputStream inputStream) throws IOException {

		// initialize partition buffers
		HashMap<Integer, ArrayList<Integer[]>>[] partitionBuffers = new HashMap[10];

		int partitionBufferSize = Math.floorDiv(BUFFER_FOR_PARTITIONS, numPartitions);
		int partitionBufferFreespace[] = new int[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			partitionBufferFreespace[i] = partitionBufferSize;
			HashMap<Integer, ArrayList<Integer[]>> vertexAdjList = new HashMap<Integer, ArrayList<Integer[]>>();
			partitionBuffers[i] = new HashMap<Integer, ArrayList<Integer[]>>();
		}
		this.partitionBuffers = partitionBuffers;
		this.partitionBufferSize = partitionBufferSize;
		this.partitionBufferFreespace = partitionBufferFreespace;

		// read the input graph edge-wise and process each edge
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		while ((ln = ins.readLine()) != null) {
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
			sendBufferEdgestoDisk_ByteFmt(partitionId);
		}
	}

	/**
	 * searches the partitionAllocationTable to find out which partition a
	 * vertex belongs (CALLED BY addEdgetoBuffer() method)
	 * 
	 * @param srcV
	 */
	private int findPartition(int srcVId) {
		int partitionId = 111;
		for (int i = 0; i < numPartitions; i++) {
			if (srcVId <= partitionAllocationTable[i]) {
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

		DataOutputStream adjListOutputStream = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(baseFilename + ".partition." + partitionId, true)));

		// Reading data
		// DataInputStream dataIn = new DataInputStream(new
		// BufferedInputStream(new FileInputStream("D:\\file.txt")));

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
		adjListOutputStream.close();

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
