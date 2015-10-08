package edu.uci.ics.cs.gdtc.engine;

import java.util.logging.Logger;

import edu.uci.ics.gdtc.GraphDTCLogger;


/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class GraphDTCEngine {
	private static final Logger logger = GraphDTCLogger.getLogger("graphQ engine");
	
	public GraphDTCEngine() {
		
	}
	
	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	public void run() {
		// 1. load partitions into memory
		loadPartitions();
		
		// 2. do computation and add edges
		doComputation();
		
		// 3. store partitions to disk
		storePartitions();
	}

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void storePartitions() {
		
	}

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void doComputation() {
		
	}

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void loadPartitions() {
		
	}
}
