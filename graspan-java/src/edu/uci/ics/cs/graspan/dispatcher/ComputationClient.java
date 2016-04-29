package edu.uci.ics.cs.graspan.dispatcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationEL.EngineEL;
import edu.uci.ics.cs.graspan.computationM.EngineM;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.GraspanTimer;
import edu.uci.ics.cs.graspan.support.MemUsageCheckThread;

public class ComputationClient {

	private static final Logger logger = GraspanLogger.getLogger("ComputationClient");

	public static void main(String args[]) throws IOException {

		String computationConfigFilename = args[0];

		/*
		 * Scan the Computer-client config file
		 */
		BufferedReader computationConfigStream = new BufferedReader(new InputStreamReader(new FileInputStream(new File(computationConfigFilename))));
		String ln;

		String[] tok;
		while ((ln = computationConfigStream.readLine()) != null) {
			tok = ln.split(" ");
			if (tok[0].compareTo("INPUT_GRAPH_FILEPATH") == 0) {
				GlobalParams.setBasefilename(tok[2]);
			}
			if (tok[0].compareTo("TOTAL_NUM_PARTS") == 0) {
				GlobalParams.setNumParts(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("RELOAD_PLAN") == 0) {
				GlobalParams.setReloadPlan(tok[2]);
			}
			if (tok[0].compareTo("EDC_SIZE") == 0) {
				GlobalParams.setEdcSize(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("OP_EDGE_TRACKER_INTERVAL") == 0) {
				GlobalParams.setOpEdgeTrackerInterval(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("MAX_PART_SIZE_POST_NEW_EDGES") == 0) {
				GlobalParams.setPartMaxPostNewEdges(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("NEW_EDGE_NODE_SIZE") == 0) {
				GlobalParams.setNewEdgesNodeSize(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("COMPUTATION_LOGIC") == 0) {
				GlobalParams.setComputationLogic(tok[2]);
			}
			if (tok[0].compareTo("NUM_OF_THREADS") == 0) {
				GlobalParams.setNumThreads(Integer.parseInt(tok[2]));
			}
			if (tok[0].compareTo("<END_CONFIG_FILE_BODY>") == 0) {
				break;
			}
		}

		computationConfigStream.close();
		
		MemUsageCheckThread memUsage = new MemUsageCheckThread();
		memUsage.start();

		logger.info("Starting computation.");
		logger.info("Total number of partitions: " + GlobalParams.getNumParts());
		logger.info("Number of parts per computation: " + GlobalParams.getNumPartsPerComputation());
		logger.info("Reload plan: " + GlobalParams.getReloadPlan());

		if (GlobalParams.getComputationLogic().compareTo("LINEAR_SCAN_OF_LLISTS") == 0) {
			EngineEL engine = new EngineEL();
			engine.run();
			logger.info("Total number of new edges created: " + engine.get_totalNewEdgs());
		} else if (GlobalParams.getComputationLogic().compareTo("SMART_MERGE") == 0) {
			
			logger.info("Starting Smart Merge Computation");
			GraspanTimer sm_comp = new GraspanTimer(System.currentTimeMillis());			
			
			EngineM engine = new EngineM();
			engine.run();
			logger.info("Total number of new edges created: " + engine.get_totalNewEdgs());
			
			logger.info("Finished Smart Merge Computation");
			
			sm_comp.calculateDuration(System.currentTimeMillis());
			
			logger.info("Smart Merge Computation took " + GraspanTimer.getDurationInHMS(sm_comp.getDuration()));
		}
		
		MemUsageCheckThread.memoryUsageOutput.close();
		memUsage.stop();
	}
}