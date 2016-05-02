package edu.uci.ics.cs.graspan.dispatcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
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
				GlobalParams.setBasefilename(tok[2].trim());
			}
			if (tok[0].compareTo("TOTAL_NUM_PARTS") == 0) {
				GlobalParams.setNumParts(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("INPUT_GRAPH_TYPE") == 0) {
				GlobalParams.setInputGraphType(tok[2].trim());
			}
			// NEED TO ENSURE INPUT GRAPH NUMBERING STARTS FROM 1 OR 0
			if (tok[0].compareTo("INPUT_GRAPH_NUMBERING_STARTS_FROM") == 0) {
				GlobalParams.setFirstVertexID(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("PREPROCESSING_OPERATION")==0){
				GlobalParams.setPPOperation(tok[2].trim());
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
		
		if (GlobalParams.getPPOperation().compareTo("+eRULE") == 0)
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
			logger.info("Edge Adding from Erules took: "+ Utilities.getDurationInHMS(System.currentTimeMillis()-eAdd_start));
		}

		else if (GlobalParams.getPPOperation().compareTo("GenParts") == 0) 
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
		}
	
//		MemUsageCheckThread.memoryUsageOutput.close();
//		memUsage.stop();
	}

}