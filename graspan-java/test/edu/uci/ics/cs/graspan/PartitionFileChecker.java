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
	
	String baseFilename;
	int reqVertexId;
	
	public PartitionFileChecker(String basefilename, int reqVertexId){
		this.baseFilename=basefilename;
		this.reqVertexId=reqVertexId;
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {
		PartitionFileChecker pfchecker = new PartitionFileChecker(args[0],Integer.parseInt(args[1]));
		pfchecker.run();
	}
	
	
	public void run() throws IOException {
		// readPartitionFile(baseFilename);
	
		//read all partition files
		for (int i = 0; i < 10; i++) {
			System.out.println("partition " + i);
			readPartitionFile();
		}

	}

	/**
	 * Reads and prints the contents of the partition files
	 * 
	 * @param baseFilename
	 * @throws IOException
	 */
	private void readPartitionFile() throws IOException {
		DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFilename)));
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
