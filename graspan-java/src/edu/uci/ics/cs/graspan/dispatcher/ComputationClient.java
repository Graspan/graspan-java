package edu.uci.ics.cs.graspan.dispatcher;

import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computation.Engine;
import edu.uci.ics.cs.graspan.datastructures.GlobalParams;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

public class ComputationClient {

	private static final Logger logger = GraspanLogger.getLogger("ComputationClient");

	public static void main(String args[]) throws IOException {

		GlobalParams.setBasefilename(args[0]);
		GlobalParams.setNumParts(Integer.parseInt(args[1]));
		GlobalParams.setNumPartsPerComputation(Integer.parseInt(args[2]));
		GlobalParams.setReloadPlan(args[3]);
		GlobalParams.setPreservePlan(args[4]);

		Engine engine = new Engine();
		engine.run();
	}
}