package edu.uci.ics.gdtc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.uci.ics.cs.gdtc.edgecomputation.BasicScheduler;
import edu.uci.ics.cs.gdtc.engine.GraphDTCEngine;
import edu.uci.ics.cs.gdtc.preproc.PartitionGenerator;

/**
 * 
 * @author Aftab
 *
 */
public class MainGraphDTC {

	/**
	 * The main program which calls methods for performing all phases of
	 * GraphDTC
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String baseFilename = args[0];
		int numInputPartitions = Integer.parseInt(args[1]);
		int numPartsPerComputation = Integer.parseInt(args[2]);
		System.out.println(">Input graph: " + args[0]);
		System.out.println(">Requested number of partitions to generate: " + args[1]);

		// PREPROCESSING
		// use PartitionGenerator to create the partitions
		System.out.println("Start preprocessing");
		long preprocStartTime = System.nanoTime();
		PartitionGenerator partgenerator = initPartGenerator(baseFilename, numInputPartitions);
		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));
		System.out.println("End of preprocessing");
		long preprocDuration = System.nanoTime() - preprocStartTime;
		System.out.println(">Total preprocessing time (nanoseconds): " + preprocDuration);

		// COMPUTATION
		System.out.println("Start computation");
		// determine the partitions to load in memory
		System.out.print("Initializing scheduler... ");
		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler(numInputPartitions);
		System.out.print("Done\n");

		GraphDTCEngine engine = new GraphDTCEngine(baseFilename, 
				basicScheduler.getPartstoLoad(numPartsPerComputation));
		engine.run();
		// load the partitions to memory
//		NewEdgeComputer newEdgeComputer = new NewEdgeComputer();

		// TODO use a loop here as determined by scheduler
//		newEdgeComputer.loadPartitions(baseFilename, basicScheduler.getPartstoLoad(numPartsPerComputation));

		// compute new edges
		// NewEdgeComputer newEdgeComputer = new NewEdgeComputer();
		// newEdgeComputer.computeNewEdges(partLoader.partEdgeArrays,
		// partLoader.partEdgeValArrays,
		// partLoader.partOutDegrees);

		// compute new edges from the partitions loaded

	}

	/**
	 * Initialize the PartitionGenerator-program
	 * 
	 * @param inputGraphPath
	 * @param numPartitions
	 */
	protected static PartitionGenerator initPartGenerator(String inputGraphPath, int numPartitions) throws IOException {
		return new PartitionGenerator(inputGraphPath, numPartitions);
	}
}
