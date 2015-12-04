package edu.uci.ics.cs.graspan;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * 
 * @author Aftab
 *
 */
public class PartitionFileChecker {

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {
		String baseFilename = args[0];
		readPartitionFile(baseFilename);
	}

	/**
	 * Reads and prints the contents of the partition files
	 * 
	 * @param baseFilename
	 * @throws IOException
	 */
	private static void readPartitionFile(String baseFilename) throws IOException {
		DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFilename)));

		while (true) {
			try {
				// srcVId
				System.out.println("src "+dataIn.readInt());
				System.out.println("-----");

				// count (number of destVs from srcV in the current list)
				int count = dataIn.readInt();
				System.out.println("# "+count);
				System.out.println("-----");

				for (int i = 0; i < count; i++) {
					// dstVId
					System.out.println("dst "+dataIn.readInt());

					// edge value
					System.out.println("val "+dataIn.readByte());
					
				}
				System.out.println("=====");

			} catch (Exception exception) {
				break;
			}
		}
		dataIn.close();
	}

}
