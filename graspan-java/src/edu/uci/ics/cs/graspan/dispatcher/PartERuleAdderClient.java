package edu.uci.ics.cs.graspan.dispatcher;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.preproc.GraphERuleEdgeAdderAndSorter;
import edu.uci.ics.cs.graspan.preproc.PartERuleAdder;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.Utilities;

public class PartERuleAdderClient {

	private static final Logger logger = GraspanLogger.getLogger("PartERuleAdderClient");

	public static void main(String args[]) throws IOException {

		GlobalParams.setBasefilename(args[0]);
		GlobalParams.setNumParts(Integer.parseInt(args[1]));
		GlobalParams.setHasEdgeVals(args[2].trim());
		GlobalParams.setFirstVertexID(Integer.parseInt(args[3]));

		GrammarChecker.loadGrammars(new File(GlobalParams.getBasefilename() + ".grammar"));

		logger.info("PREPROCESSING: Start computing and adding edges from eRules...");
		long eAdd_start = System.currentTimeMillis();

		for (int partId = 0; partId < GlobalParams.getNumParts(); partId++) {
			PartERuleAdder partERadder = new PartERuleAdder();
			partERadder.loadAndProcessParts(partId);
		}

		logger.info("PREPROCESSING: Finished computing and adding edges from eRules.");

		logger.info("Edge Adding from Erules took: " + Utilities.getDurationInHMS(System.currentTimeMillis() - eAdd_start));

	}
}
