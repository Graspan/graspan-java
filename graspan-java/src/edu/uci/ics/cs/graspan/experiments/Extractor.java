package edu.uci.ics.cs.graspan.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

public class Extractor {
	
	private long numOfEdgesStart, numOfVs, numOfEdgesEnd, numOfNewEdges;
	private double gcDuration_s;
	private String pgenDuration_hms, eredgsDuration_hms, totalDuration_hms, gcDuration_hms;
	
	private static final Logger logger = GraspanLogger.getLogger("Extractor");
	private String finishTime_hms;
	private long finishTime_s, totalMemUsage;
	
	private long writeDuration, readDuration;
	
	private int numRounds;
	private int numRoundsWithRepartitioning;

	// VERY IMPORTANT
	// args[0] -- pp.eadd.output
	// args[1] -- pp.pgen.output
	// args[2] -- comp.output
	// args[3] -- comp.gctimes.output
	// args[4] -- comp.memusage.output
	// args[5] -- basefilename
	public static void main(String args[]) throws IOException {
		Extractor extractor = new Extractor();
		extractor.run(args);
	}
	
	public void run(String args[]) throws FileNotFoundException, IOException{
		scan_pp_eadd(args);
		scan_pp_pgen(args);
		scan_comp(args);
		
		this.finishTime_s=  (long)  Integer.parseInt(finishTime_hms.split(",")[0])*60*60 //hour --> sec
							+ Integer.parseInt(finishTime_hms.split(",")[1])*60    //min  --> sec
							+ Integer.parseInt(finishTime_hms.split(",")[2]);
		
		
		numOfEdgesEnd = numOfEdgesStart + numOfNewEdges;
		
		scan_gctimes(args);
		gcDuration_hms = Utilities.getDurationInHMS((long)gcDuration_s*1000);
		
		scan_pmap(args);
		
		// write the file
		printData(args);
		
	}
	
	private void printData(String[] args) throws IOException{
			PrintWriter gptabStrm;
			gptabStrm = new PrintWriter(new BufferedWriter(new FileWriter("graspanPerformance.Table.csv", true)));
			
			gptabStrm.println(args[5]+","+this.numOfEdgesStart+","+this.numOfVs+","+this.numOfEdgesEnd+","+this.numOfVs+","+this.eredgsDuration_hms+","+
			this.pgenDuration_hms+","+this.numRounds+","+this.numRoundsWithRepartitioning+","+this.totalDuration_hms+","+this.gcDuration_hms+","+this.totalMemUsage+","+this.readDuration+","+this.writeDuration);
			
			gptabStrm.close();
	}
	
	private void scan_pmap(String[] args) throws FileNotFoundException, IOException {
		String ln,time ;
		String[] tok, timeparts;
		long currentTime_s=0, totalMemUsage=0;
		
		BufferedReader pmapStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[4]))));

		while ((ln = pmapStrm.readLine()) != null) {
			if (!ln.contains("data")) {
				tok = ln.split("\\s+");
				time = tok[0];
				timeparts = time.split("-");
				
				currentTime_s=  (long)  Integer.parseInt(timeparts[3])*60*60 //hour --> sec
						+ Integer.parseInt(timeparts[4])*60    //min  --> sec
						+ Integer.parseInt(timeparts[5]);
				
				if (currentTime_s>this.finishTime_s){
					break;
				}
				
				if (tok.length > 1) {
					totalMemUsage += Long.parseLong(tok[2]);
				}
			}
		}
		this.totalMemUsage = totalMemUsage;
		pmapStrm.close();
	}

	private void scan_gctimes(String[] args) throws FileNotFoundException, IOException {
		String ln;
		String[] tok;
		long gcDuration_s=0;
		BufferedReader gcTimeStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[3]))));

		while ((ln = gcTimeStrm.readLine()) != null) {
			if (ln.contains("secs")) {
				tok = ln.split(" ");
//				logger.info(tok[tok.length-2]);
				gcDuration_s+=Double.parseDouble(tok[tok.length - 2]);
			}
		}
		this.gcDuration_s=gcDuration_s;
		gcTimeStrm.close();
	}

	private void scan_comp(String[] args) throws FileNotFoundException, IOException {
		String ln;
		String[] tok;
		int numRounds=0;
		int numRoundsWithoutRepart=0;
		long numOfNewEdges=0;
		long readDuration = 0, writeDuration=0;
		String totalDuration_hms="",finishTime_hms="";
		BufferedReader compStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[2]))));

		while ((ln = compStrm.readLine()) != null) {
			if (ln.contains("output.round")) {
				tok = ln.split("\\|\\|");
//				logger.info(Arrays.deepToString(tok));
				numRounds = Integer.parseInt(tok[tok.length-1].split(",")[0]);
			}
			if (ln.contains("No Parts Repartitioned.")) {
				tok = ln.split("\\|\\|");
//				logger.info(Arrays.deepToString(tok));
				numRoundsWithoutRepart++;
			}
			if (ln.contains("Total Num of New Edges:")) {
				tok = ln.split(" ");
				numOfNewEdges = Long.parseLong(tok[tok.length - 1]);
			}
			if (ln.contains("Computation took")) {
				tok = ln.split(" ");
				totalDuration_hms = tok[tok.length - 1];
				
				//finding finish time
				String finishTime[] = tok[0].split(":");
				int hour = Integer.parseInt(finishTime[0]);
				if (tok[1].compareTo("PM")==0){
					hour+=12;
				}
				finishTime_hms=hour+","+finishTime[1]+","+finishTime[2];
			}
			if (ln.contains("output.IO") & ln.contains("write")){
				tok = ln.split("\\s+");
				String[] outputIO = tok[tok.length-1].split(",");
				writeDuration+=Long.parseLong(outputIO[outputIO.length-1]);
			}
			if (ln.contains("output.IO") & ln.contains("read")){
				tok = ln.split("\\s+");
				String[] outputIO = tok[tok.length-1].split(",");
				readDuration+=Long.parseLong(outputIO[outputIO.length-1]);
			}
		}
		this.numRounds=numRounds;
		this.numRoundsWithRepartitioning=numRounds-numRoundsWithoutRepart;
		this.numOfNewEdges=numOfNewEdges;
		this.totalDuration_hms=totalDuration_hms;
		this.finishTime_hms=finishTime_hms;
		this.writeDuration=writeDuration;
		this.readDuration=readDuration;
		compStrm.close();
	}

	private void scan_pp_pgen(String[] args) throws FileNotFoundException, IOException {
		String ln;
		String[] tok;
		long numOfEdgesStart=0,numOfVs=0;
		String pgenDuration_hms="";
		BufferedReader pp_pgenStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[1]))));

		while ((ln = pp_pgenStrm.readLine()) != null) {
			if (ln.contains("The total number of edges in graph")) {
				tok = ln.split(" ");
				numOfEdgesStart = Long.parseLong(tok[tok.length - 1]);
			}
			if (ln.contains("Total number of vertices in input graph:")) {
				tok = ln.split(" ");
				numOfVs = Long.parseLong(tok[tok.length - 1]);
			}
			if (ln.contains("Generating partitions took:")) {
				tok = ln.split(" ");
				pgenDuration_hms = tok[tok.length - 1];
			}
		}
		this.numOfEdgesStart=numOfEdgesStart;
		this.numOfVs=numOfVs;
		this.pgenDuration_hms=pgenDuration_hms;
		pp_pgenStrm.close();
	}

	private void scan_pp_eadd(String[] args) throws FileNotFoundException, IOException {
		String ln;
		String tok[],eredgsDuration_hms="";
		BufferedReader pp_eredgs = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[0]))));

		while ((ln = pp_eredgs.readLine()) != null) {
			if (ln.contains("Edge Adding from Erules took:")) {
				tok = ln.split(" ");
				eredgsDuration_hms = tok[tok.length - 1];
			}
		}
		this.eredgsDuration_hms=eredgsDuration_hms;
		pp_eredgs.close();
	}
}
