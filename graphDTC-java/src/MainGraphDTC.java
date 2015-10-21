import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.uci.ics.cs.gdtc.edgecomputation.BasicScheduler;
import edu.uci.ics.cs.gdtc.edgecomputation.NewEdgeComputer;
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

		/**
		 * PREPROCESSING (PARTITIONING)
		 */
		// initialize Partition Generator Program
		System.out.println("Start preprocessing");
		long preprocStartTime = System.nanoTime();
		PartitionGenerator partgenerator = initPartGenerator(baseFilename, numInputPartitions);

		// generate degrees file
		long degGenStartTime = System.nanoTime();
		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		System.out.println(">Total time for generating degrees file (nanoseconds): " + degGenDuration);

		// creating the partitions
		long creatingPartsStartTime = System.nanoTime();
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));
		long creatingPartsDuration = System.nanoTime() - creatingPartsStartTime;
		System.out.println(">Total time for creating partitions (nanoseconds):" + creatingPartsDuration);

		System.out.println("End of preprocessing");
		long preprocDuration = System.nanoTime() - preprocStartTime;
		System.out.println(">Total preprocessing time (nanoseconds): " + preprocDuration);

		/**
		 * SCHEDULING AND LOADING (LOAD PARTITIONS TO MEMORY)
		 */
		System.out.println("Start scheduling and loading");
		System.out.print("Initializing scheduler... ");
		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler(numInputPartitions);
		System.out.print("Done\n");

		System.out.print("Initializing loader... ");
		NewEdgeComputer newEdgeComputer = new NewEdgeComputer();
		System.out.print("Done\n");

		// TODO use a loop here as determined by scheduler
		long loadingStartTime = System.nanoTime();
		newEdgeComputer.loadPartitions(baseFilename, basicScheduler.getPartstoLoad(numPartsPerComputation),
				numInputPartitions);
		long loadingDuration = System.nanoTime() - loadingStartTime;
		System.out.println(
				">Total time for loading " + numPartsPerComputation + " partitions (nanoseconds): " + loadingDuration);

		/**
		 * COMPUTATION (GENERATE NEW EDGES)
		 */
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
