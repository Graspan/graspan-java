package edu.uci.ics.cs.gdtc.dispatcher;

import java.io.IOException;

import edu.uci.ics.cs.gdtc.engine.Engine;
import edu.uci.ics.cs.gdtc.scheduler.BasicScheduler;

public class NewEdgeComputer {

	public static void main(String args[]) throws IOException {

		String baseFilename = args[0];
		int numInParts = Integer.parseInt(args[1]);
		int numPartsPerComputation = Integer.parseInt(args[2]);

		System.out.print("Initializing scheduler... ");
		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler(numInParts);
		System.out.print("Done\n");

		/**
		 * COMPUTATION (GENERATE NEW EDGES)
		 */
		Engine engine = new Engine(baseFilename, basicScheduler.getPartstoLoad(numPartsPerComputation));
		engine.run();
	}
}