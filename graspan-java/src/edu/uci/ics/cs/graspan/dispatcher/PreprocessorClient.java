package edu.uci.ics.cs.graspan.dispatcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

		String baseFilename = args[0];
		int numInParts = Integer.parseInt(args[1]);
		logger.info("Input graph: " + args[0]);
		logger.info("Requested number of partitions to generate: " + args[1]);

		// initialize Partition Generator Program
		logger.info("Starting preprocessing...");
		long preprocStartTime = System.nanoTime();
		Preprocessor partgenerator = initPartGenerator(baseFilename, numInParts);

		// generate degrees file
		long degGenStartTime = System.nanoTime();
		partgenerator.generateGraphDegs(new FileInputStream(new File(baseFilename)));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		logger.info(">Total time for generating degrees file (nanoseconds): " + degGenDuration);

		// creating the partitions
		long creatingPartsStartTime = System.nanoTime();
		partgenerator.createPartVIntervals();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));
		partgenerator.generatePartDegs();
		long creatingPartsDuration = System.nanoTime() - creatingPartsStartTime;
		logger.info(">Total time for creating partitions (nanoseconds):" + creatingPartsDuration);

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