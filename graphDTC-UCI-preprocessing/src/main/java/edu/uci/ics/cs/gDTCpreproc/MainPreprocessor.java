package edu.uci.ics.cs.gDTCpreproc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class MainPreprocessor {
	/**
	 * Initialize the PartitionGenerator-program.
	 * 
	 * @param graphName
	 * @param numParts
	 */
	protected static PartitionGenerator createPartition(String graphName, int numParts) throws IOException {
		return new PartitionGenerator(graphName, numParts);
	}

	public static void main(String[] args) throws Exception {
		String baseFilename = args[0];
		int nParts = Integer.parseInt(args[1]);

//		creates the empty partitionGenerator data structure.
		PartitionGenerator partgenerator = createPartition(baseFilename, nParts);

		partgenerator.generateDegrees(new FileInputStream(new File(baseFilename)));
		partgenerator.createPartAllocTab(nParts);
		partgenerator.pgen(new FileInputStream(new File(baseFilename)));

	}
}
