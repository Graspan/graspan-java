package edu.uci.ics.graspan.oracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.support.GraspanLogger;

public class WholeGraphComputer {

	private static final Logger logger = GraspanLogger.getLogger("PreprocessorClient");

	public static int gph[][] = new int[1000][2];
	public static int newEdgesMain[][] = new int[1000][2];

	public static void main(String args[]) throws IOException {
		String baseFilename = args[0];
		logger.info("Input graph: " + args[0]);

		// initialize the graph data structures with -1
		for (int i = 0; i < gph.length; i++) {
			gph[i][0] = -1;
			gph[i][1] = -1;
		}
		for (int i = 0; i < newEdgesMain.length; i++) {
			newEdgesMain[i][0] = -1;
			newEdgesMain[i][1] = -1;
		}
		logger.info("Completed initialization of graph data structures.");

		BufferedReader ins = new BufferedReader(new InputStreamReader(new FileInputStream(new File(baseFilename))));
		String ln;

		// read in the original graph
		int i = 0;
		while ((ln = ins.readLine()) != null) {
			String[] tok = ln.split("\t");
			gph[i][0] = Integer.parseInt(tok[0]);
			gph[i][1] = Integer.parseInt(tok[1]);
			i++;
		}
		logger.info("Finished reading original graph into memory.");

		int nextGphPos = i;
		logger.info("Number of edges in original graph: " + nextGphPos);

		int iterationNo = 1;
		int[][] graphToCompute = gph;
		boolean isNewEdgeAdded = false;
		do {
			printGraph();
			logger.info("Computation iteration number: " + iterationNo);
			isNewEdgeAdded = performDTCComputation(graphToCompute);
			logger.info("New edges added in this iteration?: " + isNewEdgeAdded);
			for (int j = 0; j < newEdgesMain.length; j++) {
				if (newEdgesMain[j][0] == -1 | newEdgesMain[j][1] == -1)
					break;
				gph[nextGphPos][0] = newEdgesMain[j][0];
				gph[nextGphPos][1] = newEdgesMain[j][1];
				nextGphPos++;
			}
			iterationNo++;
		} while (isNewEdgeAdded);

	}

	public static boolean performDTCComputation(int[][] gph) {
		int newEdges[][] = new int[1000][2];
		int newEdgesMarker = 0;

		// initializing newEdges
		for (int i = 0; i < newEdges.length; i++) {
			newEdges[i][0] = -1;
			newEdges[i][1] = -1;
		}
		boolean isNewEdgeAdded = false;
		int candidateEdgeV1, candidateEdgeV2;
		for (int j = 0; j < gph.length; j++) {
			for (int k = 0; k < gph.length; k++) {
				if (gph[j][1] == gph[k][0] & gph[j][1] != -1 & gph[k][0] != -1) {
					candidateEdgeV1 = gph[j][0];
					candidateEdgeV2 = gph[k][1];
					boolean edgeExists = false;

					// check whether this edge already exists in the original
					// graph
					for (int m = 0; m < gph.length; m++) {
						if (candidateEdgeV1 == gph[m][0] & candidateEdgeV2 == gph[m][1]) {
							// logger.info("Edge already exists: " +
							// candidateEdgeV1 + "---->" + candidateEdgeV2);
							edgeExists = true;
							break;
						}
					}

					// check whether this edge already exists in the newEdges
					// data structure
					for (int m = 0; m < newEdges.length; m++) {
						if (candidateEdgeV1 == newEdges[m][0] & candidateEdgeV2 == newEdges[m][1]) {
							// logger.info("Edge already exists: " +
							// candidateEdgeV1 + "---->" + candidateEdgeV2);
							edgeExists = true;
							break;
						}
					}

					if (!edgeExists) {
						// logger.info("New edge found: " + candidateEdgeV1 +
						// "---->" + candidateEdgeV2);
						// add the edge
						newEdges[newEdgesMarker][0] = candidateEdgeV1;
						newEdges[newEdgesMarker][1] = candidateEdgeV2;
						newEdgesMarker++;
						isNewEdgeAdded = true;
					}
				}
			}
		}
		logger.info("Number of new edges added in this iteration: " + newEdgesMarker);
		newEdgesMain = newEdges;
		printNewEdges();
		return isNewEdgeAdded;
	}

	public static void printGraph() {
		logger.info("Printing graph.");
		String s = "\n";
		for (int i = 0; i < gph.length; i++) {
			if (gph[i][0] == -1)
				break;
			s = s + gph[i][0] + "---->" + gph[i][1] + "\n";
		}
		logger.info(s);
	}

	public static void printNewEdges() {
		logger.info("Printing new Edges.");
		String s = "\n";
		for (int i = 0; i < newEdgesMain.length; i++) {
			if (newEdgesMain[i][0] == -1)
				break;
			s = s + newEdgesMain[i][0] + "---->" + newEdgesMain[i][1] + "\n";
		}
		logger.info(s);
	}
}
