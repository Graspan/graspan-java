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
	
	public PartitionFileChecker(String basefilename){
//	public PartitionFileChecker(String basefilename, int reqVertexId, int numParts){
		this.baseFilename=basefilename;
//		this.reqVertexId=reqVertexId;
//		this.numParts=numParts;
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String args[]) throws IOException {
		PartitionFileChecker pfchecker = new PartitionFileChecker(args[0]);
//		PartitionFileChecker pfchecker = new PartitionFileChecker(args[0],Integer.parseInt(args[1]),Integer.parseInt(args[2]));
		pfchecker.run();
	}
	
	
	public void run() throws IOException {
		// readPartitionFile(baseFilename);
	
		//read all partition files
//		for (int i = 0; i < numParts; i++) {
		int i=2;
//		logger.info("partition " + i);
//			System.out.println("partition " +partId);
			readAndPrint(i);
//		}

	}

	/**
	 * Reads and prints the contents of the partition files
	 * 
	 * @param baseFilename
	 * @throws IOException
	 */
	private void readAndPrint(int partId) throws IOException {
		DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFilename + ".partition." + partId)));
		int srcVId, destVId;
		byte edgVal;
		String edgValStr="";
		while (true) {
			try {
				
				srcVId = dataIn.readInt();
			
//				if (srcVId==reqVertexId)
				{
					// srcVId
//					System.out.println("src " + srcVId);//Adjacency List Print Style
//					System.out.println("-----");//Adjacency List Print Style

					// count (number of destVs from srcV in the current list)
					int count = dataIn.readInt();
//					System.out.println("# " + count);//Adjacency List Print Style
//					System.out.println("-----");//Adjacency List Print Style

					for (int i = 0; i < count; i++) {
						// dstVId
						destVId=dataIn.readInt();
//						System.out.println("dst " + destVId);//Adjacency List Print Style

						// edge value
						edgVal = dataIn.readByte();
//						System.out.println("val " + edgVal);//Adjacency List Print Style
						
						//edgeval to number conversion:
						
						if (edgVal==0){
							edgValStr="n";
						}
						if (edgVal==1){
							edgValStr="e";
						}
						
						System.out.println(srcVId+"\t"+destVId+"\t"+edgValStr);//Edge List Print Style
						

					}
//					System.out.println("=====");//Adjacency List Print Style
				}

			} catch (Exception exception) {
				break;
			}
		}
		dataIn.close();
	}

}
