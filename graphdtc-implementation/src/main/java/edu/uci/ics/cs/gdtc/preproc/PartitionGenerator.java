package edu.uci.ics.cs.gdtc.preproc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;
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
	private TreeMap<Integer, Integer> degreesMap;
	private int[] partitionTable;

	private DataOutputStream[] partitionStreams;

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

		// initialize partition table and create empty partition files
		partitionTable = new int[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			partitionTable[i] = 0;
			new FileWriter(baseFilename + ".partition." + i, true);
		}
	}

	/**
	 * Scans the entire graph and counts the out degrees of all the vertices.
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
		
//		PrintWriter degreeOutputStream = new PrintWriter(
//				new BufferedWriter(new FileWriter(baseFilename + ".degrees", true)));
		
		PrintWriter degreeOutputStream = new PrintWriter(baseFilename + ".degrees", "UTF-8");
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			degreeOutputStream.println(pair.getKey() + "\t" + pair.getValue());
		}
		degreeOutputStream.close();
		this.degreesMap = degreesMap;
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

		Iterator it = degreesMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			intervalHeadVertexId = (Integer) pair.getKey();
			intervalEdgeCount = intervalEdgeCount + (Integer) pair.getValue();

			// whenever the total degree is greater than the intervalMax,
			// assign the partition interval head to the current scanned vertex 
			if (intervalEdgeCount > intervalMax & !isLastPartition(partTabIdx)) {
				
				partitionTable[partTabIdx] = intervalHeadVertexId;
				System.out.println(partitionTable[partTabIdx]);
				System.out.println(intervalEdgeCount);
				System.out.println();
				intervalEdgeCount = 0;
				partTabIdx++;
			}

			// when we have reached last partition, assign the partition interval head to the last vertex
			else if (isLastPartition(partTabIdx)) {
				intervalHeadVertexId = degreesMap.lastKey();
				partitionTable[partTabIdx] = intervalHeadVertexId;
				break;
			}
		}
	}

	/**
	 * check whether we have reached the last partition (used by)
	 * allocateVIntervalstoPartitions() method
	 * 
	 * @param partTabIdx
	 * @return
	 */
	private boolean isLastPartition(int partTabIdx) {
		return (partTabIdx == (partitionTable.length - 1) ? true : false);
	}

	/**
	 * Adds edges to the partition files
	 */
	public void addEdge(int src, int dest, byte edgeValue, long partMax) throws IOException {
	}

	/**
	 * generates the partitions
	 * 
	 * @param inputStream
	 * @param format
	 *            graph input format
	 * @throws IOException
	 */
	public void pgen(InputStream inputStream) throws IOException {
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		long partMax = numEdges / numPartitions;
		String ln;
		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				String[] tok = ln.split("\t");

				// Edge list: <src> <dst> <value>
				this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), Byte.parseByte(tok[2]), partMax);
			}
		}
	}

}
