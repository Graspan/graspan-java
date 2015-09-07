package edu.uci.ics.cs.gdtc.preproc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * @author Aftab
 *
 */
public class PartitionGenerator {

	private int numPartitions;
	private long numEdges;
	private String baseFilename;
	private TreeMap<Integer, Integer> outDegreesMap;
	private int[] partitionAllocationTable;
	private static final int BUFFER_FOR_PARTITIONS = 10;
	private int partitionBufferSize;

	// Each partition buffer consists of an adjacency list.
	// ArrayList<HashMap<srcVertexID, <[destVertexID1,edgeValue1],
	// [destVertexID2,edgeValue2]..........>>
	private ArrayList<LinkedHashMap<Integer, ArrayList<Integer[]>>> partitionBuffers;

	private int[] partitionBufferFreespace;

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

		// initialize partition allocation table and create empty partition
		// files
		partitionAllocationTable = new int[numPartitions];
//		for (int i = 0; i < numPartitions; i++) {
//			partitionAllocationTable[i] = 0;
//			new FileWriter(baseFilename + ".partition." + i, true);
//		}
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
		TreeMap<Integer, Integer> degreesMap = new TreeMap<Integer, Integer>();

		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				String[] tok = ln.split("\t");

				int src = Integer.parseInt(tok[0]);

				if (!degreesMap.containsKey(src)) {
					degreesMap.put(src, 1);
				} else {
					degreesMap.put(src, degreesMap.get(src) + 1);
				}
				numEdges++;
			}
		}
		this.numEdges = numEdges;

		// Save the degrees on disk
		Iterator it = degreesMap.entrySet().iterator();

		PrintWriter degreeOutputStream = new PrintWriter(baseFilename + ".degrees", "UTF-8");
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			degreeOutputStream.println(pair.getKey() + "\t" + pair.getValue());
		}
		degreeOutputStream.close();
		this.outDegreesMap = (TreeMap<Integer, Integer>) Collections.unmodifiableMap(degreesMap);
	}

	/**
	 * Allocates vertex intervals to partitions.
	 */
	public void allocateVIntervalstoPartitions() {

		long avgEdgesPerPartition = Math.floorDiv(numEdges, numPartitions);
		long intervalMax = (long) (avgEdgesPerPartition * 0.9);

		int intervalHeadVertexId = 0;
		int intervalEdgeCount = 0;
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
		ArrayList<LinkedHashMap<Integer, ArrayList<Integer[]>>> partitionBuffers = new ArrayList<LinkedHashMap<Integer, ArrayList<Integer[]>>>();

		int partitionBufferSize = Math.floorDiv(BUFFER_FOR_PARTITIONS, numPartitions);
		int partitionBufferFreespace[] = new int[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			partitionBufferFreespace[i] = partitionBufferSize;
			LinkedHashMap<Integer, ArrayList<Integer[]>> vertexAdjList = new LinkedHashMap<Integer, ArrayList<Integer[]>>();
			partitionBuffers.add(vertexAdjList);
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
				processEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), Integer.parseInt(tok[2]));
			}
		}

		// transfer any remaining edges in the buffer to files.
		for (int i = 0; i < numPartitions; i++) {
			transferBufferEdgestoDisk(i);
		}
	}

	/**
	 * Checks each edge, finds the appropriate partition, adds them to the
	 * corresponding partition buffer, and then calls transferBufferEdgestoDisk
	 * when the buffer is full
	 * 
	 * @param srcVId
	 * @param dstVId
	 * @param edgeValue
	 * @throws IOException
	 */
	private void processEdge(int srcVId, int dstVId, int edgeValue) throws IOException {

		int partitionId = findPartition(srcVId);

		// obtain the adjacencyList from the relevant partition buffer
		LinkedHashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partitionBuffers.get(partitionId);

		// obtain the relevant srcVIdrow row from the adjacencyList (if it
		// already exists)
		Integer[] destEdgeValPair = new Integer[2];
		destEdgeValPair[0] = dstVId;
		destEdgeValPair[1] = edgeValue;

		if (vertexAdjList.containsKey(srcVId)) {
			vertexAdjList.get(srcVId).add(destEdgeValPair);
		}
		else{
			ArrayList<Integer[]> srcVIdRow = new ArrayList<Integer[]>();
			srcVIdRow.add(destEdgeValPair);
			vertexAdjList.put(srcVId, srcVIdRow);
		}

		// store the dstVId and edge value in an array and add it to
		// srcVIdrow

		/*
		 * update the adjacency list and store it back in the buffer in place of
		 * the original adjacency list if there is space available in the buffer
		 */
		
		if (!isPartitionBufferFull(partitionId)) {
//			partitionBuffers.set(partitionId, vertexAdjList);
			partitionBufferFreespace[partitionId] = partitionBufferFreespace[partitionId] - 1;
		} else {
			transferBufferEdgestoDisk(partitionId);
//			partitionBuffers.set(partitionId, vertexAdjList);
//			partitionBufferFreespace[partitionId] = this.partitionBufferSize;
		}

	}

	/**
	 * searches the partitionAllocationTable to find out which partition a
	 * vertex belongs (called by processEdge() method)
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
	 * Checks whether partition buffer is full (called by processEdge() method)
	 * 
	 * @param partitionId
	 * @return
	 */
	private boolean isPartitionBufferFull(int partitionId) {
		return (partitionBufferFreespace[partitionId] == 0 ? true : false);
	}

	/**
	 * Transfers the buffer edges to disk (performs the actual write operation)
	 * 
	 * @param partitionId
	 * @throws IOException
	 */
	private void transferBufferEdgestoDisk(int partitionId) throws IOException {
		int srcId, destId, count;
		byte edgeValue;

		PrintWriter adjListOutputStream = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".partition." + partitionId, true)));

		// adjListOutputStream.println();

		// obtain the adjacencyList from the relevant partition buffer
		LinkedHashMap<Integer, ArrayList<Integer[]>> vertexAdjList = partitionBuffers.get(partitionId);
		Iterator<Map.Entry<Integer, ArrayList<Integer[]>>> it = vertexAdjList.entrySet().iterator();
		while (it.hasNext()) {

			Map.Entry<Integer, ArrayList<Integer[]>> pair = it.next();

			// write the srcId
			srcId = pair.getKey();
			adjListOutputStream.println(srcId);

			// get the relevant srcVIdrow row from the adjacencyList
			ArrayList<Integer[]> srcVIdRow = pair.getValue();

			// write the count
			count = srcVIdRow.size();
			adjListOutputStream.println(count);

			// write the destId edgeValue pair
			for (int i = 0; i < srcVIdRow.size(); i++) {
				Integer[] destIdEdgeVal = srcVIdRow.get(i);
				destId = destIdEdgeVal[0];
				edgeValue = Byte.parseByte(destIdEdgeVal[1].toString());
				adjListOutputStream.println(destId);
				adjListOutputStream.println(edgeValue);
			}
		}
		adjListOutputStream.close();

		// empty the buffer
		vertexAdjList.clear();
//		partitionBuffers.set(partitionId, vertexAdjList);
		partitionBufferFreespace[partitionId] = partitionBufferSize;
	}
}
