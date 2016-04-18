package edu.uci.ics.graspan.oracle;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class GraphPartComparator {

	int numParts;
	String basefilename;

	private Map<Integer, HashSet<Pair>> gph_NaiveCompOP;
	private Map<Integer, HashSet<Pair>> gph_EngineOP;

	 private Map<Integer, HashSet<Pair>> gph_NaiveDiffEngine;
	 private Map<Integer, HashSet<Pair>> gph_EngineDiffNaive;

	private long totalEdgesLoaded_EngineOutput;
	private long totalEdgesLoaded_NaiveCompGraph;
	 private long totalEdges_gph_NaiveDiffEngine;
	 private long totalEdges_gph_EngineDiffNaive;

	public GraphPartComparator(String basefilename, int numParts) {
		this.basefilename = basefilename;
		this.numParts = numParts;
		this.gph_NaiveCompOP = new HashMap<Integer, HashSet<Pair>>();
		this.gph_EngineOP = new HashMap<Integer, HashSet<Pair>>();

	}

	public static void main(String args[]) throws IOException {

		GraphPartComparator gpc = new GraphPartComparator(args[0], Integer.parseInt(args[1]));
		gpc.run();

	}

	private void run() throws IOException {

		loadNaiveCompGraphOP();
		System.out.println("loaded naive computer graph");

		for (int i = 0; i < numParts; i++) {
			loadPartsOPfromEngine(i);
		}
		System.out.println("loaded engine computer graph");


		 gph_EngineDiffNaive = genGraphDiff(gph_EngineOP, gph_NaiveCompOP,
		 "engine");
		 System.out.println("found gph_EngineDiffNaive");
		
		 gph_NaiveDiffEngine = genGraphDiff(gph_NaiveCompOP, gph_EngineOP,
		 "naive");
		 System.out.println("found gph_NaiveDiffEngine");
		 
		System.out.println("totalEdges_EngineOutput: " + totalEdgesLoaded_EngineOutput);
		System.out.println("totalEdges_NaiveCompGraph: " + totalEdgesLoaded_NaiveCompGraph);
		System.out.println("totalEdges_gph_NaiveDiffEngine: " + totalEdges_gph_NaiveDiffEngine);
		System.out.println("totalEdges_gph_EngineDiffNaive: " + totalEdges_gph_EngineDiffNaive);
		
		System.out.println("gph_EngineDiffNaive :-");
		
		for (Integer src : gph_EngineDiffNaive.keySet()) {
			HashSet<Pair> dstValPairs = gph_EngineDiffNaive.get(src);
			for (Pair pr : dstValPairs){
				System.out.println(src + "\t" + pr.target + "\t" + pr.evalue);
			}
		}
		
		System.out.println("gph_NaiveDiffEngine :-");

		for (Integer src : gph_NaiveDiffEngine.keySet()) {
			HashSet<Pair> dstValPairs = gph_NaiveDiffEngine.get(src);
			for (Pair pr : dstValPairs) {
				System.out.println(src + "\t" + pr.target + "\t" + pr.evalue);
			}
		}

	}

	private Map<Integer, HashSet<Pair>> genGraphDiff(Map<Integer, HashSet<Pair>> mapA, Map<Integer, HashSet<Pair>> mapB,
			String firstgraph) {

		Map<Integer, HashSet<Pair>> diffMap = new HashMap<Integer, HashSet<Pair>>();
		
		for (int srcA : mapA.keySet()) {
			HashSet<Pair> dstValPairsA = mapA.get(srcA);
			HashSet<Pair> dstValPairsB = mapB.get(srcA);
			
			HashSet<Pair> dstValPairsA_copy = new HashSet<Pair>();
			HashSet<Pair> dstValPairsB_copy = new HashSet<Pair>();
			
			for (Pair pr:dstValPairsA){
				dstValPairsA_copy.add(pr);
			}
			
			if (dstValPairsB!=null){
			for (Pair pr:dstValPairsB){
				dstValPairsB_copy.add(pr);
			}
			
			dstValPairsA_copy.removeAll(dstValPairsB_copy);
			}
			diffMap.put(srcA, dstValPairsA_copy);
			
			if (firstgraph.compareTo("naive") == 0) {
				this.totalEdges_gph_NaiveDiffEngine += dstValPairsA_copy.size();
			}
			if (firstgraph.compareTo("engine") == 0) {
				this.totalEdges_gph_EngineDiffNaive += dstValPairsA_copy.size();

		}
		}
		return diffMap;
		
		
		

	}

	/**
	 * load input graph into memory for further computation
	 */
	private void loadNaiveCompGraphOP() {
		BufferedReader reader;
		String line;
		try {
			reader = new BufferedReader(new FileReader(new File(this.basefilename + ".final")));
			while ((line = reader.readLine()) != null) {
				if (!line.isEmpty()) {
					String[] tok = line.split("\t");// NOTE: MAKE SURE INPUT
													// FILE IS TAB DELIMITED
					int srcId = Integer.parseInt(tok[0]);
					int tgtId = Integer.parseInt(tok[1]);
					byte eval = Byte.parseByte(tok[2]);

					this.totalEdgesLoaded_NaiveCompGraph++;
					if (this.gph_NaiveCompOP.containsKey(srcId)) {
						boolean flag = this.gph_NaiveCompOP.get(srcId).add(new Pair(tgtId, eval));
						assert (flag);
					} else {
						HashSet<Pair> set = new HashSet<Pair>();
						set.add(new Pair(tgtId, eval));
						this.gph_NaiveCompOP.put(srcId, set);
					}
				}
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * load input partition into memory for further computation
	 * 
	 * @throws IOException
	 */
	private void loadPartsOPfromEngine(int partId) throws IOException {
		DataInputStream dataIn = new DataInputStream(
				new BufferedInputStream(new FileInputStream(basefilename + ".partition." + partId)));
		while (true) {
			try {

				int srcVId = dataIn.readInt();

				// count (number of destVs from srcV in the current list)
				int count = dataIn.readInt();

				for (int i = 0; i < count; i++) {
					int dstVId = dataIn.readInt();
					byte eval = dataIn.readByte();

					this.totalEdgesLoaded_EngineOutput++;
					if (this.gph_EngineOP.containsKey(srcVId)) {
						boolean flag = this.gph_EngineOP.get(srcVId).add(new Pair(dstVId, eval));
						assert (flag);
					} else {
						HashSet<Pair> set = new HashSet<Pair>();
						set.add(new Pair(dstVId, eval));
						this.gph_EngineOP.put(srcVId, set);
					}

				}

			} catch (Exception exception) {
				break;
			}
		}
		dataIn.close();

	}

	static class Pair {
		final int target;
		final byte evalue;

		public Pair(int t, byte eval) {
			this.target = t;
			this.evalue = eval;
		}

		public int hashCode() {
			int r = 1;
			r = r * 31 + this.target;
			r = r * 31 + this.evalue;
			return r;
		}

		public boolean equals(Object o) {
			return (o instanceof Pair) && (((Pair) o).target == this.target) && (((Pair) o).evalue == this.evalue);
		}

	}

}
