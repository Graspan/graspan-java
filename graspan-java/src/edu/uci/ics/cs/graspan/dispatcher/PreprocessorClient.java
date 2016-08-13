package edu.uci.ics.cs.graspan.dispatcher;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.preproc.GraphERuleEdgeAdder;
import edu.uci.ics.cs.graspan.preproc.Preprocessor;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

/**
 * This program performs preprocessing of the input graph to generate partitions
 * 
 * @author Aftab
 * 
 */
public class PreprocessorClient {

	private static final Logger logger = GraspanLogger.getLogger("PreprocessorClient");

	public static void main(String[] args) throws IOException {

		String preprocessorConfigFilename = args[0];

		/*
		 * Scan the Computer-client config file
		 */
		BufferedReader preprocessorConfigStream = new BufferedReader(new InputStreamReader(new FileInputStream(new File(preprocessorConfigFilename))));
		String ln;

		String[] tok;
		while ((ln = preprocessorConfigStream.readLine()) != null) {
			tok = ln.split(" ");
			if (tok[0].compareTo("INPUT_GRAPH_FILEPATH") == 0) {
//				GlobalParams.setBasefilename(tok[2].trim());//use when running through EclipseIDE TODO
				GlobalParams.setBasefilename(args[1]);//use when running as .jar
			}
			if (tok[0].compareTo("TOTAL_NUM_PARTS") == 0) {
				GlobalParams.setNumParts(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("INPUT_GRAPH_TYPE") == 0) {
				GlobalParams.setInputGraphType(tok[2].trim());
			}
			if (tok[0].compareTo("RELOAD_PLAN") == 0) {
				GlobalParams.setReloadPlan(tok[2]);
			}
			if (tok[0].compareTo("EDC_SIZE") == 0) {
				GlobalParams.setEdcSize(Integer.parseInt(tok[2]));
			}
			// NEED TO ENSURE INPUT GRAPH NUMBERING STARTS FROM 1 OR 0
			if (tok[0].compareTo("INPUT_GRAPH_NUMBERING_STARTS_FROM") == 0) {
				GlobalParams.setFirstVertexID(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("PREPROCESSING_OPERATION")==0){
				GlobalParams.setPPOperation(tok[2].trim());
			}
			
			// THE FOLLOWING ARE NOT USED FOR PREPROCESSING, BUT USED TO GENERATE THE CONFIG FILE 
			// FOR THE COMPUTATION
			if (tok[0].compareTo("HEAP_SIZE(GB)")==0){ 
				GlobalParams.setHeapSize(Integer.parseInt(tok[2].trim()));
			}
			if (tok[0].compareTo("PART_SIZE_CONST")==0){ 
				GlobalParams.setPSizeConst(Double.parseDouble(tok[2].trim()));
			}
			if (tok[0].compareTo("REPART_SIZE_CONST")==0){ 
				GlobalParams.setRepartConst(Double.parseDouble(tok[2].trim()));
			}
			if (tok[0].compareTo("COMPUTATION_LOGIC") == 0) { 
				GlobalParams.setComputationLogic(tok[2]);
			}
			if (tok[0].compareTo("NUM_OF_THREADS") == 0) { 
				GlobalParams.setNumThreads(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("MAX_PART_SIZE_POST_NEW_EDGES") == 0) {
				GlobalParams.setPartMaxPostNewEdges(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("ANALYSIS_TYPE") == 0) {// POINTSTO OR DATAFLOW (IF DATAFLOW NO ERULES ARE ADDED)
				GlobalParams.setAnalysisType(tok[2]);
			}
			if (tok[0].compareTo("<END_CONFIG_FILE_BODY>") == 0) {
				break;
			}
		}

		preprocessorConfigStream.close();
		
//		MemUsageCheckThread memUsage = new MemUsageCheckThread();
//		memUsage.start();
		
		logger.info("Input graph: " + GlobalParams.getBasefilename());
		logger.info("Requested # partitions to generate: " + GlobalParams.getNumParts());
		
		GrammarChecker.loadGrammars(new File(GlobalParams.getBasefilename() + ".grammar"));
		
		if (GlobalParams.getPPOperation().compareTo("erule") == 0)
		// adding edges to the graph based on eRules
		{
			logger.info("PREPROCESSING: Start computing and adding edges from eRules...");
//			GraspanTimer ppERedgeAdding = new GraspanTimer(System.currentTimeMillis());
			long eAdd_start = System.currentTimeMillis();
			
    
    		GraphERuleEdgeAdder edgeAdder = new GraphERuleEdgeAdder();
			edgeAdder.run();
				
			logger.info("PREPROCESSING: Finished computing and adding edges from eRules.");
			
//			ppERedgeAdding.calculateDuration(System.currentTimeMillis());
//			logger.info("Edge Adding from Erules took: "+ GraspanTimer.getDurationInHMS(ppERedgeAdding.getDuration()));
			logger.info("Edge Adding from Erules took: " + Utilities.getDurationInHMS(System.currentTimeMillis() - eAdd_start));
			
			
			/*
			 * Generating pp.pgen.config file for partition generation process
			 */
			
			double gSize = getSizeOfGraphWithEREdgs_MB();
			logger.info(gSize+"");
			
			double pSize_expected = getExpectedPartSize_MB();
			logger.info("pSize_expected " + pSize_expected);
			
			double gSize_to_pSizeExpected = (double) gSize/pSize_expected;
			logger.info("gSize_to_pSizeExpected " + gSize_to_pSizeExpected);
			
			int numparts_torequest = getNumOfPartsToRequest(gSize_to_pSizeExpected);
			logger.info("numparts_torequest " + numparts_torequest);
			
			double pSize_asPerNumparts_torequest = (double)gSize/numparts_torequest;
			logger.info("pSize_asPerNumparts_torequest " + pSize_asPerNumparts_torequest);
			
			double pSize_postNewEdges_MB = getPSize_postNewEdges_MB(pSize_asPerNumparts_torequest);
			logger.info("pSize_postNewEdges_MB " + pSize_postNewEdges_MB);
			
			double pSize_postNewEdges_inNumOfEdges = pSize_postNewEdges_MB * 50000;//1MB of Graph roughly equal to 50000 edges on disk
			logger.info("pSize_postNewEdges_inNumOfEdges "+pSize_postNewEdges_inNumOfEdges);
			
			printValstoPgenconfFile(numparts_torequest, pSize_postNewEdges_inNumOfEdges);
			
			logger.info("Generated pp.pgen.config");
			
		}

		else if (GlobalParams.getPPOperation().compareTo("genparts") == 0) 
		// generate pp parts
		{
			logger.info("PREPROCESSING: Start generating partitions...");
//			GraspanTimer ppPartGen = new GraspanTimer(System.currentTimeMillis());
			long pp_start = System.currentTimeMillis();
			
			// initialize Partition Generator Program
			Preprocessor partgenerator = new Preprocessor(GlobalParams.getBasefilename(), GlobalParams.getNumParts());
			partgenerator.run();

			logger.info("PREPROCESSING: Finished generating partitions.");
			
//			ppPartGen.calculateDuration(System.currentTimeMillis());
//			logger.info("Generating partitions took: "+ GraspanTimer.getDurationInHMS(ppPartGen.getDuration()));
			logger.info("Generating partitions took: " + Utilities.getDurationInHMS(System.currentTimeMillis()-pp_start));
			
			/*
			 * Generating comp.config file for computation process
			 */
			
			printValstoCompconfFile();
			
			logger.info("Generated comp.config");
			
		}
	
//		MemUsageCheckThread.memoryUsageOutput.close();
//		memUsage.stop();
	}

	private static void printValstoCompconfFile() throws IOException {
		PrintWriter compconfStrm;
		compconfStrm = new PrintWriter(new BufferedWriter(new FileWriter("comp.config", true)));
		
		compconfStrm.println("<START_CONFIG_FILE_BODY>");
		compconfStrm.println();
		compconfStrm.println("INPUT_GRAPH_FILEPATH = " + GlobalParams.getBasefilename());
		compconfStrm.println("TOTAL_NUM_PARTS = " + AllPartitions.getPartAllocTab().length);
		compconfStrm.println("MAX_PART_SIZE_POST_NEW_EDGES = " + GlobalParams.getPartMaxPostNewEdges());
		compconfStrm.println("RELOAD_PLAN = "+GlobalParams.getReloadPlan());
		compconfStrm.println("EDC_SIZE = " + GlobalParams.getEdcSize());
		compconfStrm.println("COMPUTATION_LOGIC = " + GlobalParams.getComputationLogic());
		compconfStrm.println("NUM_OF_THREADS = " + GlobalParams.getNumThreads());
		compconfStrm.println();
		compconfStrm.println("<END_CONFIG_FILE_BODY>");
		
		compconfStrm.close();
	}

	private static void printValstoPgenconfFile(int numparts_torequest, double pSize_postNewEdges_inNumOfEdges) throws IOException {
		PrintWriter pgenconfStrm;
		pgenconfStrm = new PrintWriter(new BufferedWriter(new FileWriter("pp.pgen.config", true)));
		
		pgenconfStrm.println("<START_CONFIG_FILE_BODY>");
		pgenconfStrm.println();
		pgenconfStrm.println("INPUT_GRAPH_FILEPATH = " + GlobalParams.getBasefilename() + ".eRulesAdded");
		pgenconfStrm.println("TOTAL_NUM_PARTS = " + numparts_torequest);
		pgenconfStrm.println("INPUT_GRAPH_TYPE = " + GlobalParams.getInputGraphType());
		pgenconfStrm.println("INPUT_GRAPH_NUMBERING_STARTS_FROM = " + GlobalParams.getFirstVertexID());
		pgenconfStrm.println("MAX_PART_SIZE_POST_NEW_EDGES = " + Math.round(pSize_postNewEdges_inNumOfEdges));
		pgenconfStrm.println("PREPROCESSING_OPERATION = genparts");
		pgenconfStrm.println("RELOAD_PLAN = "+GlobalParams.getReloadPlan());
		pgenconfStrm.println("EDC_SIZE = " + GlobalParams.getEdcSize());
		pgenconfStrm.println("COMPUTATION_LOGIC = " + GlobalParams.getComputationLogic());
		pgenconfStrm.println("NUM_OF_THREADS = " + GlobalParams.getNumThreads());
		pgenconfStrm.println();
		pgenconfStrm.println("<END_CONFIG_FILE_BODY>");
		
		pgenconfStrm.close();
	}

	private static double getPSize_postNewEdges_MB(double pSize_asPerNumparts_torequest) {
		double pSize_postNewEdges_MB = pSize_asPerNumparts_torequest + 
				                    ((double)pSize_asPerNumparts_torequest * GlobalParams.getRepartConst());
		return pSize_postNewEdges_MB;
	}

	private static int getNumOfPartsToRequest(double gSize_to_pSizeExpected) {
		int numparts_torequest=0;
		if (gSize_to_pSizeExpected<=2){
			numparts_torequest = 2;
		}
		else {
			numparts_torequest = (int) Math.ceil(gSize_to_pSizeExpected);
		}
		return numparts_torequest;
	}

	private static double getExpectedPartSize_MB() {
		double pSize_expected = (GlobalParams.getPSizeConst()*GlobalParams.getHeapSize()*1000)/2;
		return pSize_expected;
	}

	private static double getSizeOfGraphWithEREdgs_MB() {
		File graph = new File(GlobalParams.getBasefilename()+".eRulesAdded");
		double gSize = (double)graph.length()/1000000;
		return gSize;
	}

}