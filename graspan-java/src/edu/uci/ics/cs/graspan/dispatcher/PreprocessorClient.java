package edu.uci.ics.cs.graspan.dispatcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import edu.uci.ics.cs.graspan.preproc.Preprocessor;

/**
 * This program performs preprocessing of the input graph to generate partitions
 * 
 * @author Aftab
 *
 */
public class PreprocessorClient {

	public static void main(String[] args) throws IOException {

		String baseFilename = args[0];
		int numInParts = Integer.parseInt(args[1]);
		System.out.println(">Input graph: " + args[0]);
		System.out.println(">Requested number of partitions to generate: " + args[1]);

		// initialize Partition Generator Program
		System.out.println("Starting preprocessing...");
		long preprocStartTime = System.nanoTime();
		Preprocessor partgenerator = initPartGenerator(baseFilename, numInParts);

		// generate degrees file
		long degGenStartTime = System.nanoTime();
		partgenerator.generateGraphDegs(new FileInputStream(new File(baseFilename)));
		long degGenDuration = System.nanoTime() - degGenStartTime;
		System.out.println(">Total time for generating degrees file (nanoseconds): " + degGenDuration);

		// creating the partitions
		long creatingPartsStartTime = System.nanoTime();
		partgenerator.createPartVIntervals();
		partgenerator.writePartitionEdgestoFiles(new FileInputStream(new File(baseFilename)));
		partgenerator.generatePartDegs();
		long creatingPartsDuration = System.nanoTime() - creatingPartsStartTime;
		System.out.println(">Total time for creating partitions (nanoseconds):" + creatingPartsDuration);

		System.out.println("Preprocessing complete.");
		long preprocDuration = System.nanoTime() - preprocStartTime;
		System.out.println(">Total preprocessing time (nanoseconds): " + preprocDuration);

	}

	/**
	 * Initialize the Preprocessor-program
	 * 
	 * @param inputGraphPath
	 * @param numParts
	 */
	protected static Preprocessor initPartGenerator(String inputGraphPath, int numParts) throws IOException {
		return new Preprocessor(inputGraphPath, numParts);
	}
}