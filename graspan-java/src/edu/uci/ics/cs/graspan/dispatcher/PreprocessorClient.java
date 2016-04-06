package edu.uci.ics.cs.graspan.dispatcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

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
			if (tok[0].compareTo("INPUT_GRAPH_NUMBERING_STARTS_FROM") == 0) {
				GlobalParams.setFirstVertexID(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("<END CONFIGFILE>") == 0) {
				break;
			}
		}

		preprocessorConfigStream.close();

		logger.info("Input graph: " + GlobalParams.getBasefilename());
		logger.info("Requested # partitions to generate: " + GlobalParams.getNumParts());

		// initialize Partition Generator Program
		logger.info("Starting preprocessing...");
		long preprocStartTime = System.nanoTime();
		
		Preprocessor partgenerator = initPartGenerator(GlobalParams.getBasefilename(), GlobalParams.getNumParts());

		// generate degrees file
		long degGenStartTime = System.nanoTime();
		partgenerator.generateGraphDegs(new FileInputStream(new File(GlobalParams.getBasefilename())));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		logger.info("Total time to create degrees file (nanoseconds): " + degGenDuration);

		// creating the partitions
		long creatingPartsStartTime = System.nanoTime();
		partgenerator.createPartVIntervals();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(GlobalParams.getBasefilename())));
		partgenerator.generatePartDegs();
		long creatingPartsDuration = System.nanoTime() - creatingPartsStartTime;
		logger.info("Total time to create partitions (nanoseconds):" + creatingPartsDuration);

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
	protected static Preprocessor initPartGenerator(String inputGraphPath, int numParts) throws IOException {
		return new Preprocessor(inputGraphPath, numParts);
	}
}