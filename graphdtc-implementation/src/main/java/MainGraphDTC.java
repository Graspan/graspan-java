
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
		int numPartitions = Integer.parseInt(args[1]);

		// PREPROCESSING
		// use PartitionGenerator to create the partitions
		PartitionGenerator partgenerator = initPartGenerator(baseFilename, numPartitions);
		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));

		// COMPUTATION
//		BasicScheduler basicScheduler = new BasicScheduler();
//		basicScheduler.initScheduler(numPartitions);
		PartitionLoader partLoader = new PartitionLoader();
//		partLoader.loadPartition(baseFilename, basicScheduler.getPartstoLoad());
		
		//TEST loadPartition
		int arr[]={0,1};
		partLoader.loadPartition(baseFilename, arr);
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
