package edu.uci.ics.graspan.oracle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/*
 * Map based edge extractor from graph for a given source vertex
 */
import edu.uci.ics.cs.graspan.computationM.GrammarChecker;

/*
 * Naive Computer class using map
 */
public class GraphExtractorForVertex {
	
	private String fileName;
	
	private Map<Integer, HashSet<Pair>> graph;
	
	private long totalEdgesLoaded;
	
	private long totalNewEdgesAdded;
	
	private int req_srcVId;
	
	
	public GraphExtractorForVertex(String fileName, int req_srcVId){
		this.req_srcVId=req_srcVId;
		this.fileName = fileName;
		this.graph = new HashMap<Integer, HashSet<Pair>>();
	}
	
	public static void main(String[] args) throws IOException {
		GraphExtractorForVertex c = new GraphExtractorForVertex(args[0],Integer.parseInt(args[1]));
		c.run();
	}
	
	public void run() throws IOException{
		//load grammars
		GrammarChecker.loadGrammars(new File(this.fileName + ".grammar"));
		
		//load input graph
		loadGraph();
		
		//writes all the outgoing edges of the requested source vertex
		export_requestedVertex();
		
		System.out.println("Total number of edges loaded:\t" + this.totalEdgesLoaded);
		System.out.println("Total number of edges added:\t" + this.totalNewEdgesAdded);
	}
	
	
	/**
	 * write the final graph to disk
	 */
	private void export_requestedVertex() {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(new File(this.fileName + ".final"+".vertex."+req_srcVId))));
			for(int srcId: this.graph.keySet()){
				if (srcId == req_srcVId){
				for(Pair p: this.graph.get(srcId)){
					out.println(srcId + "\t" + p.target + "\t" + p.evalue);
				}
				}
			}
			
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}

	/**
	 * load input graph into memory for further computation
	 */
	private void loadGraph() {
		// TODO Auto-generated method stub
		BufferedReader reader;
		String line;
		try {
			reader = new BufferedReader(new FileReader(new File(this.fileName)));
			while((line = reader.readLine()) != null){
				if (!line.isEmpty()) {
					String[] tok = line.split("\t");//NOTE: MAKE SURE INPUT FILE IS TAB DELIMITED
					int srcId = Integer.parseInt(tok[0]);
					int tgtId = Integer.parseInt(tok[1]);
					byte eval = GrammarChecker.getValue(tok[2].trim());
					
					this.totalEdgesLoaded++;
					if(this.graph.containsKey(srcId)){
						boolean flag = this.graph.get(srcId).add(new Pair(tgtId, eval));
						assert(flag);
					}
					else{
						HashSet<Pair> set = new HashSet<Pair>();
						set.add(new Pair(tgtId, eval));
						this.graph.put(srcId, set);
					}
				}
			}
			
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	static class Pair{
		final int target;
		final byte evalue;
		
		public Pair(int t, byte eval){
			this.target = t;
			this.evalue = eval;
		}
		
		public int hashCode(){
			int r = 1;
			r = r * 31 + this.target;
			r = r * 31 + this.evalue;
			return r;
		}
		
		public boolean equals(Object o){
			return (o instanceof Pair) && (((Pair) o).target == this.target) && (((Pair) o).evalue == this.evalue);
		}
		
	}
}

