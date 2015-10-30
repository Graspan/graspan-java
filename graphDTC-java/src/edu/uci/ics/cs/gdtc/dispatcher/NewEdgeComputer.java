package edu.uci.ics.cs.gdtc.dispatcher;
import java.io.IOException;

import edu.uci.ics.cs.gdtc.edgecomputer.BasicScheduler;
import edu.uci.ics.cs.gdtc.edgecomputer.PartitionLoader;
import edu.uci.ics.cs.gdtc.engine.Engine;

public class NewEdgeComputer {
	
	public static void main(String args[]) throws IOException {

		String baseFilename = args[0];
		int numInParts = Integer.parseInt(args[1]);
		int numPartsPerComputation = Integer.parseInt(args[2]);
		
		System.out.print("Initializing scheduler... ");
		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler(numInParts);
		System.out.print("Done\n");

//		System.out.print("Initializing loader... ");
//		PartitionLoader partLoader = new PartitionLoader();
//		System.out.print("Done\n");

//		long loadingStartTime = System.nanoTime();
//		partLoader.loadParts(baseFilename, basicScheduler.getPartstoLoad(numPartsPerComputation),
//				numInParts);
//		long loadingDuration = System.nanoTime() - loadingStartTime;
//		System.out.println(
//				">Total time for loading " + numPartsPerComputation + " partitions (nanoseconds): " + loadingDuration);
		

		/**
		 * COMPUTATION (GENERATE NEW EDGES)
		 */
		Engine engine = new Engine(baseFilename, 
				basicScheduler.getPartstoLoad(numPartsPerComputation));
		engine.run();
		// compute new edges
		// PartitionLoader newEdgeComputer = new PartitionLoader();
		// newEdgeComputer.computeNewEdges(partLoader.partEdgeArrays,
		// partLoader.partEdgeValArrays,
		// partLoader.partOutDegrees);

		// compute new edges from the partitions loaded
	}
}