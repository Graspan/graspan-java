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
	
	private long numEdges1, numVs1, numEdges2, numVs2, numEdgesEnd, numVsEnd, numOfNewEdges;
	private double gcDuration_s;
	private String pgen1Time_hms, eRandStime_hms, pgen2Time_hms, compTime_hms, gcTime_hms;
	
	private static final Logger logger = GraspanLogger.getLogger("Extractor");
	private String finishTime_hms;
	private long finishTime_s, totalMemUsage, maxMemUsage;
	private long maxNumIters; //the maximum number of iterations for any round
	
	private long writeDuration, readDuration;
	
	private double ioTime_s;
	
	private int numRounds;
	private int numRoundsWithRepartitioning;

	// VERY IMPORTANT
	// args[0] -- pp.pgen1.output
	// args[1] -- pp.erAndSort.output
	// args[2] -- pp.pgen2.output
	// args[3] -- comp.output
	// args[4] -- comp.gctimes.output
	// args[5] -- basefilename
	// args[6] -- io.output
	// not used -- comp.memusage.output
	public static void main(String args[]) throws IOException {
		Extractor extractor = new Extractor();
		extractor.run(args);
	}
	
	public void run(String args[]) throws FileNotFoundException, IOException{
		scan_pp_pgen1(args[0]);
		scan_pp_eadd(args[1]);
		scan_pp_pgen2(args[2]);
		
		scan_comp(args);
		
		this.finishTime_s=  (long)  Integer.parseInt(finishTime_hms.split(",")[0])*60*60 //hour --> sec
							+ Integer.parseInt(finishTime_hms.split(",")[1])*60    //min  --> sec
							+ Integer.parseInt(finishTime_hms.split(",")[2]);
		
		
		numEdgesEnd = numEdges1 + numOfNewEdges;
		numVsEnd = this.numVs2;
		
		scan_gctimes(args[4]);
		gcTime_hms = Utilities.getDurationInHMS((long)gcDuration_s*1000);
		
//		scan_pmap(args);
		scan_IO_times(args[6]);
		
		// write the file
		printData(args);
		
	}
	
	public void scan_IO_times(String iofile) throws IOException{
		String ln;
		String[] tok;
		double IO_Duration=0;
		double iostat_time=0;
		double total_time= Double.parseDouble(this.compTime_hms.split(",")[0])*60*60
				          +Double.parseDouble(this.compTime_hms.split(",")[1])*60
				          +Double.parseDouble(this.compTime_hms.split(",")[2]);
				          
		BufferedReader iostat_Strm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(iofile))));
		
		while ((ln = iostat_Strm.readLine()) != null) {
			if (ln.contains("avg-cpu")) {
				ln = iostat_Strm.readLine();// read the next line after line
											// beginning with avg-cpu
				tok = ln.split("\\s+");
//				System.out.println(ln);//TODO: TEST CODE
				IO_Duration += (double) ((double)Double.parseDouble(tok[3])/100) * 5;// IO  time off 5 seconds
				iostat_time+=5;
				if (iostat_time>total_time){
					break;
				}
			}
		}
		iostat_Strm.close();
		this.ioTime_s=IO_Duration;
		
	}
	
	private void printData(String[] args) throws IOException {
		PrintWriter gptabStrm;
		gptabStrm = new PrintWriter(new BufferedWriter(new FileWriter("graspanPerformance.Table.csv", true)));

		String head = "";
		String val = "";
		
		head += "graph,";
		val += args[5] + ",";

		head += "numOfEdges1,";
		val += this.numEdges1 + ",";
		
		head += "numVs1,";
		val += this.numVs1 + ",";
		
		head += "numOfEdges2,";
		val += this.numEdges2 + ",";
		
		head += "numVs2,";
		val += this.numVs2 + ",";
		
		head += "numEdgesEnd,";
		val += this.numEdgesEnd + ",";
		
		head += "numVsEnd,";
		val += this.numVsEnd + ",";
		
		head += "pgen1Time_h,pgen1Time_m,pgen1Time_s,";
		val += this.pgen1Time_hms + ",";

		head += "eRandStime_h,eRandStime_m,eRandStime_s,";
		val += this.eRandStime_hms + ",";
		
		head += "pgen2Time_h,pgen2Time_m,pgen2Time_s,";
		val += this.pgen2Time_hms + ",";
		
		head += "numRounds,";
		val += this.numRounds + ",";
		
		head += "numRoundsWithRepartitioning,";
		val += this.numRoundsWithRepartitioning + ",";
		
		head += "compTime_h,compTime_m,compTime_s,";
		val += this.compTime_hms + ",";
		
		head += "gcTime_h,gcTime_m,gcTime_s,";
		val += this.gcTime_hms + ",";

		head += "ioTime_s,";
		val += this.ioTime_s + ",";
		
		gptabStrm.println(head);
		gptabStrm.println(val);
		
		
//		gptabStrm.println(args[5] + "," + this.numEdges1 + "," + this.numVs1 + "," + this.numEdgesEnd + ","
//				+ this.numVs1 + "," + this.eRandStime_hms + "," + this.pgen1Time_hms + "," + this.numRounds
//				+ "," + this.numRoundsWithRepartitioning + "," + this.compTime_hms + "," + this.gcTime_hms
//				+ "," + this.maxMemUsage + "," + this.ioTime_s + "," + this.maxNumIters);

		gptabStrm.close();
	}
	
	private void scan_pmap(String[] args) throws FileNotFoundException, IOException {
		String ln,time ;
		String[] tok, timeparts;
		long currentTime_s=0, totalMemUsage=0, maxMemUsage=0;
		
		BufferedReader pmapStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[4]))));

		while ((ln = pmapStrm.readLine()) != null) {
			if (!ln.contains("data")) {
				tok = ln.split("\\s+");
				time = tok[0];
				timeparts = time.split("-");
				
				currentTime_s=  (long)  Integer.parseInt(timeparts[3])*60*60 //hour --> sec
						+ Integer.parseInt(timeparts[4])*60    //min  --> sec
						+ Integer.parseInt(timeparts[5]);
				
				if (currentTime_s > this.finishTime_s){
					break;
				}
				
				if (tok.length > 1) {
					if (Long.parseLong(tok[2]) > maxMemUsage) {
						maxMemUsage = Long.parseLong(tok[2]);
					}
//					totalMemUsage += Long.parseLong(tok[2]);
				}
			}
		}
//		this.totalMemUsage = totalMemUsage;
		this.maxMemUsage = maxMemUsage;
		pmapStrm.close();
	}

	private void scan_gctimes(String gcfile) throws FileNotFoundException, IOException {
		String ln;
		String[] tok;
		double gcDuration_s=0;
		BufferedReader gcTimeStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(gcfile))));

		while ((ln = gcTimeStrm.readLine()) != null) {
			if (ln.contains("secs")) {
				tok = ln.split(" ");
//				logger.info(args[5]);
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
		long numIters=0, maxNumIters=0;
		long readDuration = 0, writeDuration=0;
		String totalDuration_hms="",finishTime_hms="";
		String[] roundOutputComponent=null; //round#,h,m,s,#edges
		String[] iterationOutputComponent=null; //iteration#, round#, h,m,s, #edges
		BufferedReader compStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[3]))));

//		PrintWriter edgesPRound;
//		edgesPRound = new PrintWriter(new BufferedWriter(new FileWriter("edgesPerRound.Table.csv", true)));
//		edgesPRound.println(args[5]);
		
		while ((ln = compStrm.readLine()) != null) {
			
			if (ln.contains("output.iteration")) {
				tok = ln.split("\\|\\|");
				iterationOutputComponent = tok[tok.length-1].split(",");
				numIters = Long.parseLong(iterationOutputComponent[1]);
				if (numIters>maxNumIters){
					maxNumIters=numIters;
				}
			}
			
			if (ln.contains("output.round")) {
				tok = ln.split("\\|\\|");
//				logger.info(Arrays.deepToString(tok));
				roundOutputComponent = tok[tok.length-1].split(",");
				numRounds = Integer.parseInt(roundOutputComponent[0]);
//				numRounds = Integer.parseInt(tok[tok.length-1].split(",")[0]);
//				edgesPRound.println(numRounds + "," + roundOutputComponent[roundOutputComponent.length-1] );
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
		
		this.maxNumIters=maxNumIters;
		this.numRounds=numRounds;
		this.numRoundsWithRepartitioning=numRounds-numRoundsWithoutRepart;
		this.numOfNewEdges=numOfNewEdges;
		this.compTime_hms=totalDuration_hms;
		this.finishTime_hms=finishTime_hms;
		this.writeDuration=writeDuration;
		this.readDuration=readDuration;
//		edgesPRound.close();
		compStrm.close();
	}

	private void scan_pp_pgen1(String ppoutput) throws FileNotFoundException, IOException {
		String ln;
		String[] tok;
		long numOfEdgesStart=0,numOfVs=0;
		String pgenDuration_hms="";
		BufferedReader pp_pgenStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(ppoutput))));

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
		this.numEdges1=numOfEdgesStart;
		this.numVs1=numOfVs;
		this.pgen1Time_hms=pgenDuration_hms;
		pp_pgenStrm.close();
	}
	
	private void scan_pp_pgen2(String ppoutput) throws FileNotFoundException, IOException {
		String ln;
		String[] tok;
		long numOfEdgesStart=0,numOfVs=0;
		String pgenDuration_hms="";
		BufferedReader pp_pgenStrm = new BufferedReader(new InputStreamReader(new FileInputStream(new File(ppoutput))));

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
		this.numEdges2=numOfEdgesStart;
		this.numVs2=numOfVs;
		this.pgen2Time_hms=pgenDuration_hms;
		pp_pgenStrm.close();
	}

	private void scan_pp_eadd(String eaddoutput) throws FileNotFoundException, IOException {
		String ln;
		String tok[],eredgsDuration_hms="";
		BufferedReader pp_eredgs = new BufferedReader(new InputStreamReader(new FileInputStream(new File(eaddoutput))));

		while ((ln = pp_eredgs.readLine()) != null) {
			if (ln.contains("Edge Adding from Erules took:")) {
				tok = ln.split(" ");
				eredgsDuration_hms = tok[tok.length - 1];
			}
		}
		this.eRandStime_hms=eredgsDuration_hms;
		pp_eredgs.close();
	}
}
