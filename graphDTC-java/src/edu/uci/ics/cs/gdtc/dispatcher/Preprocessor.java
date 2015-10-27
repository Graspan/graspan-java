package edu.uci.ics.cs.gdtc.dispatcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.uci.ics.cs.gdtc.engine.PartitionGenerator;

/**
 * This program performs preprocessing of the input graph to generate partitions
 * 
 * @author Aftab
 *
 */
public class Preprocessor {

	public static void main(String[] args) throws IOException {

		String baseFilename = args[0];
		int numInParts = Integer.parseInt(args[1]);
		System.out.println(">Input graph: " + args[0]);
		System.out.println(">Requested number of partitions to generate: " + args[1]);

		// initialize Partition Generator Program
		System.out.println("Starting preprocessing...");
		long preprocStartTime = System.nanoTime();
		PartitionGenerator partgenerator = initPartGenerator(baseFilename, numInParts);

		// generate degrees file
		long degGenStartTime = System.nanoTime();
		partgenerator.generateGraphDegs(new FileInputStream(new File(baseFilename)));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		System.out.println(">Total time for generating degrees file (nanoseconds): " + degGenDuration);

		// creating the partitions
		long creatingPartsStartTime = System.nanoTime();
		partgenerator.allocateVIntervalstoPartitions();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));
		partgenerator.generatePartDegs();
		long creatingPartsDuration = System.nanoTime() - creatingPartsStartTime;
		System.out.println(">Total time for creating partitions (nanoseconds):" + creatingPartsDuration);

		System.out.println("Preprocessing complete.");
		long preprocDuration = System.nanoTime() - preprocStartTime;
		System.out.println(">Total preprocessing time (nanoseconds): " + preprocDuration);

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
