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
		int nParts = Integer.parseInt(args[1]);

		PartitionGenerator partgenerator = createPartition(baseFilename, nParts);
		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.pgen(new FileInputStream(new File(baseFilename)));
	}

	/**
	 * Initialize the PartitionGenerator-program.
	 * 
	 * @param inputGraphPath
	 * @param numPartitions
	 */
	protected static PartitionGenerator createPartition(String inputGraphPath, int numPartitions)
			throws IOException {
		return new PartitionGenerator(inputGraphPath, numPartitions);
	}
}
