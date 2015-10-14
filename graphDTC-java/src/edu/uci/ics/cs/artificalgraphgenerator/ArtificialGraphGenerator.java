package edu.uci.ics.cs.artificalgraphgenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * This program generates a random set of distinct edges (srcId,dstId,edgeVal)
 * 
 * @author Aftab
 *
 */
public class ArtificialGraphGenerator {

	static final int MAX_VERTEX_VALUE = 10000000;
	static final int MAX_EDGE_VALUE = 256;
	static final int MAX_NUM_OF_EDGES = 10000000;

	public static void main(String args[]) throws IOException {

		Random rand = new Random();
		int numbers[] = new int[3];// = { 1000000, 1000000, 256 };
		Integer decPlaces[] = new Integer[2];
		HashMap<Long, Integer[]> edgeMap = new HashMap<Long, Integer[]>();

		// writing to map
		for (int iteration = 0; iteration < MAX_NUM_OF_EDGES; iteration++) {

			numbers[0] = rand.nextInt(MAX_VERTEX_VALUE + 1) + 1;
			numbers[1] = rand.nextInt(MAX_VERTEX_VALUE + 1) + 1;
			numbers[2] = rand.nextInt(MAX_EDGE_VALUE + 1) + 1;

			// storing the number of decimal places of 2nd and 3rd numbers
			for (int i = 1; i < 3; i++) {
				int decPlaceCount = 0;
				int a1 = numbers[i];
				while (a1 > 0) {
					a1 = a1 / 10;
					decPlaceCount++;
				}
				decPlaces[i - 1] = decPlaceCount;
			}
			// for (int i = 0; i < 2; i++) {
			// System.out.print(decPlaces[i] + " ");
			// }

			// combining the numbers
			long a = numbers[0] * 10 * (decPlaces[0] + decPlaces[1]) + numbers[1] * 10 * (decPlaces[1]) + numbers[2];
			long component1 = numbers[0], component2 = numbers[1], component3 = numbers[2], combinedNum;
			for (int i = 0; i < decPlaces[0] + decPlaces[1]; i++) {
				component1 = component1 * 10;
			}
			for (int i = 0; i < decPlaces[1]; i++) {
				component2 = component2 * 10;
			}
			combinedNum = component1 + component2 + component3;
			// System.out.println(combinedNum);

			// adding the numbers to map (to ensure non-duplication)
			edgeMap.put(combinedNum, decPlaces);
		}

		/*
		 * Retrieving the numbers and writing them to file
		 */
		PrintWriter edgeOutputStream = new PrintWriter(new BufferedWriter(new FileWriter(args[0], true)));

		Iterator<Map.Entry<Long, Integer[]>> it = edgeMap.entrySet().iterator();

		while (it.hasNext()) {

			Map.Entry<Long, Integer[]> pair = it.next();

			// retrieve the combinedNumber and the decimalPlaces array from
			// the
			// map
			long combinedNum = pair.getKey();
			decPlaces = pair.getValue();

			// retrieving srcVId
			long temp = 0;
			temp = combinedNum;
			for (int i = 0; i < decPlaces[0] + decPlaces[1]; i++) {
				temp = temp / 10;
			}
			long srcVId = temp;
			// System.out.println("Retrieved srcVId: " + srcVId);

			// retrieving destVId
			for (int i = 0; i < decPlaces[0] + decPlaces[1]; i++) {
				temp = temp * 10;
			}
			long lastTwoCombinedNum = combinedNum - temp;
			// System.out.println(lastTwoCombinedNum);
			temp = lastTwoCombinedNum;
			for (int i = 0; i < decPlaces[1]; i++) {
				temp = temp / 10;
			}
			long destVId = temp;
			// System.out.println("Retrieved destVId: " + destVId);

			// retrieving edgeVal
			for (int i = 0; i < decPlaces[1]; i++) {
				temp = temp * 10;
			}
			long edgeVal = lastTwoCombinedNum - temp;
			// System.out.println("Retrieved edgeVal: " + edgeVal);

			// writing the edge to file
			edgeOutputStream.print(srcVId);
			edgeOutputStream.print("\t");
			edgeOutputStream.print(destVId);
			edgeOutputStream.print("\t");
			edgeOutputStream.print(edgeVal);
			edgeOutputStream.println();

		}
		edgeOutputStream.close();

	}
}
