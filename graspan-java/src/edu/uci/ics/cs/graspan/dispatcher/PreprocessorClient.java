package edu.uci.ics.cs.graspan.dispatcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.preproc.GraphERuleEdgeAdder;
import edu.uci.ics.cs.graspan.preproc.PartitionPreprocessor;
import edu.uci.ics.cs.graspan.preproc.Preprocessor;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

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
		BufferedReader preprocessorConfigStream = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(preprocessorConfigFilename))));
		String ln;

		String[] tok;
		while ((ln = preprocessorConfigStream.readLine()) != null) {
			tok = ln.split(" ");
			if (tok[0].compareTo("INPUT_GRAPH_FILEPATH") == 0) {
				GlobalParams.setBasefilename(tok[2]);
			}
			if (tok[0].compareTo("TOTAL_NUM_PARTS") == 0) {
				GlobalParams.setNumParts(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("INPUT_GRAPH_TYPE") == 0) {
				GlobalParams.setInputGraphType(tok[2]);
			}
			// NEED TO ENSURE INPUT GRAPH NUMBERING STARTS FROM 1 OR 0
			if (tok[0].compareTo("INPUT_GRAPH_NUMBERING_STARTS_FROM") == 0) {
				GlobalParams.setFirstVertexID(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("<END_CONFIG_FILE_BODY>") == 0) {
				break;
			}
		}

		preprocessorConfigStream.close();

		logger.info("Input graph: " + GlobalParams.getBasefilename());
		logger.info("Requested # partitions to generate: " + GlobalParams.getNumParts());
		
		GrammarChecker.loadGrammars(new File(GlobalParams.getBasefilename() + ".grammar"));
		//--------------------------------------------------------------------------------------------------------
		// adding edges to the graph based on eRules
		
//		GraphERuleEdgeAdder edgeAdder = new GraphERuleEdgeAdder();
//		edgeAdder.run();
		
		//--------------------------------------------------------------------------------------------------------
		// initialize Partition Generator Program
		logger.info("Starting preprocessing...");
		long preprocStartTime = System.nanoTime();
		Preprocessor partgenerator = new Preprocessor(GlobalParams.getBasefilename(), GlobalParams.getNumParts());
		partgenerator.run();

		//---------------------------------------------------------------------------------------------------------
//		//Do further preprocessing of each partition //NOT REQUIRED
//		logger.info("Preprocessing each partition...");
//		PartitionPreprocessor partPreprocessor = new PartitionPreprocessor();
//		for (int partId = 0; partId < GlobalParams.getNumParts(); partId++) {
//			partPreprocessor.loadAndProcessParts(partId);
//		}
//		
		logger.info("Preprocessing complete.");
		long preprocDuration = System.nanoTime() - preprocStartTime;
		logger.info("Total preprocessing time (nanoseconds): " + preprocDuration);
	
	}

	/**
	 * Initialize the Preprocessor-program
	 * 
	 * @param inputGraphPath
	 * @param numParts
	 */
//	protected static Preprocessor initPartGenerator(String inputGraphPath, int numParts) throws IOException {
//		return new Preprocessor(inputGraphPath, numParts);
//	}
}