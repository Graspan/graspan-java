package edu.uci.ics.cs.gdtc.edgecomputer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.uci.ics.cs.gdtc.preproc.PartitionGenerator;

/**
 * 
 * @author Aftab
 *INCOMPLETE
 */
public class PartitionScheduler {
	int numParts;
	

	/**
	 * Constructor
	 */
	public PartitionScheduler() {
		int arr[][][] = new int[numParts][numParts][2];

		
	}

	private void computePartitionPercentage(int partitionId, int[] partitionAllocationTable, InputStream inputStream)
			throws IOException {

		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		String ln;
		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				int srcVId= Integer.parseInt(ln);
				int count=Integer.parseInt(ins.readLine());
				int[] destVertices=new int[count];
				for (int i=0;i<count;i++){
					destVertices[i]=Integer.parseInt(ins.readLine());
					ins.readLine();
				}
			}
		}
	}

}
