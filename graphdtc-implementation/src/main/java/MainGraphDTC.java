
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.uci.ics.cs.gdtc.edgecomputation.BasicScheduler;
import edu.uci.ics.cs.gdtc.edgecomputation.PartitionLoader;
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

		// PREPROCESSING
		// use PartitionGenerator to create the partitions
		PartitionGenerator partgenerator = initPartGenerator(baseFilename, numInputPartitions);
		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));

		// COMPUTATION
		// TODO use a loop here as determined by scheduler

		// determine the partitions to load in memory
		BasicScheduler basicScheduler = new BasicScheduler();
		basicScheduler.initScheduler(numInputPartitions);

		// load the partitions to memory
		PartitionLoader partLoader = new PartitionLoader();
		partLoader.loadPartition(baseFilename, basicScheduler.getPartstoLoad(numPartsPerComputation));

		/*
		 * TEST PartitionLoader
		 */
//		 int arr[]={0,1};
//		 partLoader.loadPartition(baseFilename, arr);

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
