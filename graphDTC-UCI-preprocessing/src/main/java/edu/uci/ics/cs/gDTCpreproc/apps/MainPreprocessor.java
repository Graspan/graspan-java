package edu.uci.ics.cs.gDTCpreproc.apps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

import edu.uci.ics.cs.gDTCpreproc.ChiFilenames;
import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.ConsolePrinter;
import edu.uci.ics.cs.gDTCpreproc.datablocks.GenericIntegerConverter;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.PartitionGenerator;

public class MainPreprocessor {
	static ConsolePrinter cp = new ConsolePrinter();

	private static Logger logger = ChiLogger.getLogger("mainPreprocessor");

	/**
	 * Initialize the PartitionGenerator-program.
	 * 
	 * @param graphName
	 * @param numParts
	 * @return
	 * @throws IOException
	 */
	protected static PartitionGenerator createPartition(String graphName, int numParts) throws IOException {
		return new PartitionGenerator(graphName, numParts, new GenericIntegerConverter(4),
				new GenericIntegerConverter(1));
	}

	public static void main(String[] args) throws Exception {
//		System.out.println("Free memory (bytes): " + 
//				  Runtime.getRuntime().freeMemory());
//		
//		System.exit(0);
//		int[] arr2=new int[1000000000];
//		byte[] arr3=new byte[1000000000];
		HashMap<Integer, Integer> vOutDegs = new HashMap<Integer,Integer>();
//		System.out.println(Integer.MAX_VALUE);
//		System.exit(0);
		String baseFilename = args[0];
		int nParts = Integer.parseInt(args[1]);

		/*
		 * creates the empty partitiongenerator data structure. Empty
		 * "shovel" #of each set = # input shards
		 */
		PartitionGenerator partgenerator = createPartition(baseFilename, nParts);

		/*
		 * checks whether the partition files already exist, if not the
		 * partitions are filled up with edges
		 */
		if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nParts)).exists()) {
			partgenerator.genDegrees(new FileInputStream(new File(baseFilename)));
//			partgenerator.pgen(new FileInputStream(new File(baseFilename)));
		} else {
			logger.info("Found partitions -- no need to preprocess");
		}

	}
}
