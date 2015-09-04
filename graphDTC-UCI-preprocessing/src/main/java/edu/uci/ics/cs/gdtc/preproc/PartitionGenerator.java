package edu.uci.ics.cs.gdtc.preproc;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class PartitionGenerator {

	private int nParts;
	private int numEdges;
	private TreeMap<Integer, Integer> vOutDegs;
	private LinkedHashMap<Integer, Integer> partAllocTable;
	private LinkedHashMap<Integer, ArrayList<Integer>>[] partMap;

	private DataOutputStream[] partitionStreams;

	/**
	 * Constructor
	 * 
	 * @param baseFilename
	 * @param nParts
	 * @throws IOException
	 */
	public PartitionGenerator(String baseFilename, int nParts) throws IOException {
		this.nParts = nParts;

//		the edges are stored in partitions.
		partitionStreams = new DataOutputStream[nParts];
		for (int i = 0; i < nParts; i++) {

//			Empty "partition" files are created"
			partitionStreams[i] = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(baseFilename + ".partition." + i)));
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
		TreeMap<Integer, Integer> vOutDegs = new TreeMap<Integer, Integer>();

		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				String[] tok = ln.split("\t");

				int src = Integer.parseInt(tok[0]);

				if (!vOutDegs.containsKey(src)) {
					vOutDegs.put(src, 1);
				} else {
					vOutDegs.put(src, vOutDegs.get(src) + 1);
				}
				numEdges++;
			}
		}
		this.numEdges = (int) numEdges;

//		Save the degrees file in disk
		Iterator it = vOutDegs.entrySet().iterator();
		PrintWriter writer = new PrintWriter("C:/Users/Aftab/workspace/graphdtc/graphDTC-UCI-preprocessing/degrees.txt",
				"UTF-8");
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();
			writer.println(pair.getKey() + "\t" + pair.getValue());
		}

		writer.close();
		this.vOutDegs = vOutDegs;
	}
	
	/**
	 * 
	 * @param nParts
	 * @throws IOException
	 */
	public void createPartAllocTab(int nParts) throws IOException {

		int partSize = numEdges / nParts;

		int partNo = 0;
		int degSum = 0;

		LinkedHashMap<Integer, Integer> partAllocTable = new LinkedHashMap<Integer, Integer>();
		/*
		 * Scanning the degrees map to assign vertices to the partition
		 * allocation table
		 */
		Iterator vOutDegIt = vOutDegs.entrySet().iterator();
		while (vOutDegIt.hasNext()) {
			
			Map.Entry pair = (Map.Entry) vOutDegIt.next();
			System.out.println(pair.getKey() +" "+pair.getValue());

			if (partNo < nParts) {
				/*
				 * Handling the case for a vertex of degree > partSize (Required
				 * if Method 2 for partSize Calc is used)
				 */
				if ((Integer) pair.getValue() > partSize) {

					/*
					 * Ensure that a vertex with degree larger than partSize is
					 * added to a partition of it's own
					 */
					if (!partAllocTable.containsKey(partNo)) {
						partAllocTable.put((Integer) pair.getKey(), partNo);
						degSum = 0;
						partNo++;
					}
				}

				degSum = degSum + (Integer) pair.getValue();
				if (degSum < partSize) {
					partAllocTable.put((Integer) pair.getKey(), partNo);
				} else {
					partNo++;
					degSum = 0;
					partAllocTable.put((Integer) pair.getKey(), partNo);
				}
			}
		}

	}

	/**
	 * Adds edges to the partition files
	 */
	public void addEdge(int src, int dest, int edgeValue, long partMax) throws IOException {
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
		long partMax = numEdges / nParts;
		String ln;
		long lineNum = 0;
		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				lineNum++;
				String[] tok = ln.split("\t");

//				Edge list: <src> <dst> <value>
				this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), Integer.parseInt(tok[2]), partMax);
			}
		}
	}



	
}
