package edu.uci.ics.cs.gdtc.dispatcher;

import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.gdtc.engine.Engine;
import edu.uci.ics.cs.gdtc.scheduler.BasicScheduler;
import edu.uci.ics.cs.gdtc.support.GDTCLogger;
import edu.uci.ics.cs.gdtc.userinput.UserInput;

public class DTCComputer {
	
	private static final Logger logger = GDTCLogger.getLogger("graphdtc dtccomputer");

	public static void main(String args[]) throws IOException {

		UserInput.setBasefilename(args[0]);
		UserInput.setNumParts(Integer.parseInt(args[1]));
		UserInput.setNumPartsPerComputation(Integer.parseInt(args[2]));

		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler();
		logger.info("Initialized scheduler.");

		Engine engine = new Engine(basicScheduler.getPartstoLoad());
		engine.run();
	}
}