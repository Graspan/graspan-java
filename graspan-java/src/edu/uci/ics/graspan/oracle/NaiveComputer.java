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

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;

/*
 * Naive Computer class using map
 */
public class NaiveComputer {
	
	private String fileName;
	
	private Map<Integer, HashSet<Pair>> graph;
	
	private long totalEdgesLoaded;
	
	private long totalNewEdgesAdded;
	
	
	public NaiveComputer(String fileName){
		this.fileName = fileName;
		this.graph = new HashMap<Integer, HashSet<Pair>>();
	}
	
	public static void main(String[] args) throws IOException {
		NaiveComputer c = new NaiveComputer(args[0]);
		c.run();
	}
	
	public void run() throws IOException{
		//load grammars
		GrammarChecker.loadGrammars(new File(this.fileName + ".grammar"));
		
		//load input graph
		loadGraph();
		
		//add edges
		doComputation();
		
		//write the final graph to disk
		export();
		
		System.out.println("Total number of edges loaded:\t" + this.totalEdgesLoaded);
		System.out.println("Total number of edges added:\t" + this.totalNewEdgesAdded);
	}
	
	
	/**
	 * write the final graph to disk
	 */
	private void export() {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(new File(this.fileName + ".final"))));
			for(int srcId: this.graph.keySet()){
				for(Pair p: this.graph.get(srcId)){
					out.println(srcId + "\t" + p.target + "\t" + GrammarChecker.getValue(p.evalue));
				}
			}
			
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	/**
	 * 
	 */
	private void doComputation() {
		boolean newAdded_flag = false;
		int iteration = 0;
		
		do{
			//one iteration computation
			newAdded_flag = doComputationForOneIteration(iteration);
			iteration++;
		}
		while(newAdded_flag);
		
		//last iteration to guarantee real termination
		newAdded_flag = doComputationForOneIteration(iteration);
		assert(!newAdded_flag);
	}

	/**
	 * add edges for one iteration
	 * @param iteration number
	 * @return
	 */
	private boolean doComputationForOneIteration(int iteration) {
		boolean newAdded = false;
		long number_added = 0;
		
		for(int srcId: this.graph.keySet()){
			Set<Pair> newAddedSet = new HashSet<Pair>();
			HashSet<Pair> first_set = this.graph.get(srcId);
			
			for(Pair first_p: first_set){
				int first_tgt = first_p.target;
				byte first_eval = first_p.evalue;
				
				//for length 1 rule checking
				byte new_eval_1 = GrammarChecker.checkL1Rules(first_eval);
				if(new_eval_1 != -1){
					Pair newPair_1 = new Pair(first_tgt, new_eval_1);
					newAddedSet.add(newPair_1);
//					if(first_set.add(newPair_1)){
//						newAdded = true;
//						number_added++;
//					}
				}
				
				//for length 2 rule checking
				if(this.graph.containsKey(first_tgt)){
					HashSet<Pair> second_set = this.graph.get(first_tgt);
					
					for(Pair second_p: second_set){
						int second_tgt = second_p.target;
						byte second_eval = second_p.evalue;
						
						byte new_eval_2 = GrammarChecker.checkL2Rules(first_eval, second_eval);
						if(new_eval_2 != -1){
							Pair newPair_2 = new Pair(second_tgt, new_eval_2);
							newAddedSet.add(newPair_2);
//							if(first_set.add(newPair)){
//								newAdded = true;
//								number_added++;
//							}
						}
					}
				}
			}
			
			for(Pair np: newAddedSet){
				if(first_set.add(np)){
					newAdded = true;
					number_added++;
				}
			}
		}
		
		System.out.println("Number of edges added in Iteration " + iteration + ":\t" + number_added);
		this.totalNewEdgesAdded += number_added;
		
		return newAdded;
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
