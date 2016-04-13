package edu.uci.ics.cs.graspan;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * 
 * @author Aftab
 *
 */
public class PartitionFileChecker {
	
	private static final Logger logger = GraspanLogger.getLogger("PartitionFileChecker");
	String baseFilename;
	int reqVertexId;
	int numParts;
	
	public PartitionFileChecker(String basefilename, int reqVertexId, int numParts){
		this.baseFilename=basefilename;
		this.reqVertexId=reqVertexId;
		this.numParts=numParts;
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {
		PartitionFileChecker pfchecker = new PartitionFileChecker(args[0],Integer.parseInt(args[1]),Integer.parseInt(args[2]));
		pfchecker.run();
	}
	
	
	public void run() throws IOException {
		// readPartitionFile(baseFilename);
	
		//read all partition files
		for (int i = 0; i < numParts; i++) {
		logger.info("partition " + i);
//			System.out.println("partition " +partId);
			readAndPrint(i);
		}

	}

	/**
	 * Reads and prints the contents of the partition files
	 * 
	 * @param baseFilename
	 * @throws IOException
	 */
	private void readAndPrint(int partId) throws IOException {
		DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partId)));
		int srcVId;
		while (true) {
			try {
				
				srcVId = dataIn.readInt();
			
				if (srcVId==reqVertexId)
				{
					// srcVId
					System.out.println("src " + srcVId);
					System.out.println("-----");

					// count (number of destVs from srcV in the current list)
					int count = dataIn.readInt();
					System.out.println("# " + count);
					System.out.println("-----");

					for (int i = 0; i < count; i++) {
						// dstVId
						System.out.println("dst " + dataIn.readInt());

						// edge value
						System.out.println("val " + dataIn.readByte());

					}
					System.out.println("=====");
				}
				else{
					// count (number of destVs from srcV in the current list)
					int count = dataIn.readInt();

					for (int i = 0; i < count; i++) {
						dataIn.readInt();
						dataIn.readByte();
					}
				}

			} catch (Exception exception) {
				break;
			}
		}
		dataIn.close();
	}

}
