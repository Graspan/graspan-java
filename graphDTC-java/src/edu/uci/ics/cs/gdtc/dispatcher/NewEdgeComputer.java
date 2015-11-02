package edu.uci.ics.cs.gdtc.dispatcher;

import java.io.IOException;

import edu.uci.ics.cs.gdtc.engine.Engine;
import edu.uci.ics.cs.gdtc.scheduler.BasicScheduler;
import edu.uci.ics.cs.gdtc.userinput.UserInput;

public class NewEdgeComputer {

	public static void main(String args[]) throws IOException {

		UserInput.setBasefilename(args[0]); 
		UserInput.setNumParts(Integer.parseInt(args[1]));
		UserInput.setNumPartsPerComputation(Integer.parseInt(args[2]));

		System.out.print("Initializing scheduler... ");
		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler();
		System.out.print("Done\n");

		/**
		 * COMPUTATION (GENERATE NEW EDGES)
		 */
		Engine engine = new Engine(basicScheduler.getPartstoLoad());
		engine.run();
	}
}