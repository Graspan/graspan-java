package edu.uci.ics.cs.gdtc.preproc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * 
 * @author Aftab
 *
 */
public class MainPreprocessor {

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String baseFilename = args[0];
		int numPartitions = Integer.parseInt(args[1]);

		// use PartitionGenerator to create the partitions
		PartitionGenerator partgenerator = initPartGenerator(baseFilename, numPartitions);
		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));
	}

	/**
	 * Initialize the PartitionGenerator-program.
	 * 
	 * @param inputGraphPath
	 * @param numPartitions
	 */
	protected static PartitionGenerator initPartGenerator(String inputGraphPath, int numPartitions) throws IOException {
		return new PartitionGenerator(inputGraphPath, numPartitions);
	}
}
