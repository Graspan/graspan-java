package edu.uci.ics.cs.graspan.dispatcher;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.preproc.Preprocessor;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

/**
 * This program performs preprocessing of the input graph to generate partitions
 * 
 * @author Aftab
 * 
 */
public class PartGenClient {

	private static final Logger logger = GraspanLogger.getLogger("PartGenClient");

	public static void main(String[] args) throws IOException {
		
//		for (int i = 0; i < args.length; i++) {
//			logger.info("    "+i+"  "+args[i].trim());
//		}
		
		GlobalParams.setBasefilename(args[0]);
		GlobalParams.setNumParts(Integer.parseInt(args[1]));
		GlobalParams.setHasEdgeVals(args[2].trim());
		GlobalParams.setFirstVertexID(Integer.parseInt(args[3]));

		logger.info("Input graph: " + GlobalParams.getBasefilename());
		logger.info("Requested # partitions to generate: " + GlobalParams.getNumParts());

		GrammarChecker.loadGrammars(new File(GlobalParams.getBasefilename() + ".grammar"));

		logger.info("PREPROCESSING: Start generating partitions...");
		long pp_start = System.currentTimeMillis();

		// initialize Partition Generator Program
		Preprocessor partgenerator = new Preprocessor(GlobalParams.getBasefilename(), GlobalParams.getNumParts());
		partgenerator.run();

		logger.info("PREPROCESSING: Finished generating partitions.");

		logger.info("Generating partitions took: " + Utilities.getDurationInHMS(System.currentTimeMillis() - pp_start));

	}

}