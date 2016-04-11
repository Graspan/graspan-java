package edu.uci.ics.graspan.oracle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

public class WholeGraphComputer {

	private static final Logger logger = GraspanLogger.getLogger("DTC-Oracle");
	private static final int EDGE_BUFFER_SIZE = 1000;

	public static int gph[][] = new int[EDGE_BUFFER_SIZE][2];
	public static byte vals[]=new byte[EDGE_BUFFER_SIZE];
	public static int newEdgesMain[][] = new int[EDGE_BUFFER_SIZE][2];
	public static byte newValsMain[] = new byte[EDGE_BUFFER_SIZE];

	public static void main(String args[]) throws IOException {
		
		String baseFilename = init(args);
		
		//load grammar
		GrammarChecker.loadGrammars(new File(baseFilename + ".grammar"));

		// read in the original graph
		BufferedReader ins = new BufferedReader(new InputStreamReader(new FileInputStream(new File(baseFilename))));
		String ln;
		int i = 0;
		while ((ln = ins.readLine()) != null) {
			if (!ln.isEmpty()) {
				String[] tok = ln.split("\t");
				gph[i][0] = Integer.parseInt(tok[0]);
				gph[i][1] = Integer.parseInt(tok[1]);
				vals[i] = GrammarChecker.getValue(tok[2].trim());
				i++;
			}
		}

		logger.info("Finished reading original graph into memory.");

		int nextGphPos = i;
		logger.info("# edges in original graph: " + nextGphPos);

		//---------------------------------------------------------------------------------------------------------------
		//BEGINNING COMPUTATION
		
		
		int iterationNo = 1;
		
			int[][] graphToCompute = gph;
			byte[] valsToCompute = vals;

			boolean isNewEdgeAdded = false;
			do {
				// printGraph();
				logger.info("Computation iteration #: " + iterationNo);
				isNewEdgeAdded = performComputation(graphToCompute, valsToCompute);
				logger.info("New edges added in this iteration?: " + isNewEdgeAdded);
				nextGphPos = transferNewEdgestoOriginal(nextGphPos);
				iterationNo++;
			} while (isNewEdgeAdded);
			
//			printGraph();
			storePart_ActualEdges(baseFilename);

	}

	private static int transferNewEdgestoOriginal(int nextGphPos) {
		for (int j = 0; j < newEdgesMain.length; j++) {
			if (newEdgesMain[j][0] == -1 | newEdgesMain[j][1] == -1)
				break;
			gph[nextGphPos][0] = newEdgesMain[j][0];
			gph[nextGphPos][1] = newEdgesMain[j][1];
			vals[nextGphPos]=newValsMain[j];
			nextGphPos++;
		}
		return nextGphPos;
	}

	private static String init(String[] args) {
		String baseFilename = args[0];
		logger.info("Input graph: " + args[0]);

		// initialize the graph data structures with -1
		for (int i = 0; i < gph.length; i++) {
			gph[i][0] = -1;
			gph[i][1] = -1;
			vals[i]=-1;
		}
		for (int i = 0; i < newEdgesMain.length; i++) {
			newEdgesMain[i][0] = -1;
			newEdgesMain[i][1] = -1;
			newValsMain[i]=-1;
		}
		logger.info("Completed initialization of graph data structures.");
		return baseFilename;
	}
	
//	public static void performComputationL1Rule(int[][] gph, byte[] vals) {
//		int newEdges[][] = new int[EDGE_BUFFER_SIZE][2];
//		byte newVals[] = new byte[EDGE_BUFFER_SIZE];
//		int newEdgesMarker = 0;
//		
//		// initializing newEdges
//		for (int i = 0; i < newEdges.length; i++) {
//			newEdges[i][0] = -1;
//			newEdges[i][1] = -1;
//			newVals[i] = -1;
//		}
//		
//		int candidateEdgeV1, candidateEdgeV2;
//		byte edgeVal, OPEval;
//		
//		for (int j = 0; j < gph.length; j++) {
//			
//			candidateEdgeV1 = gph[j][0];
//			candidateEdgeV2 = gph[j][1];
//			edgeVal = vals[j];
//			
//			OPEval = GrammarChecker.checkL1Rules(edgeVal);
//			
//			if (OPEval==-1) continue;
//			
//			boolean edgeExists = false;
//			
//			// check whether this edge already exists in the original graph
//			edgeExists = isInGraph(gph, vals, candidateEdgeV1, candidateEdgeV2, OPEval, edgeExists);
//
//			// check whether edge exists in the new edges
//			edgeExists = isInGraph(newEdges, newVals, candidateEdgeV1, candidateEdgeV2, OPEval, edgeExists);
//			
//			if (!edgeExists) {
//				
//				// logger.info("New edge found: " + candidateEdgeV1 +  "---->" + candidateEdgeV2 + "("+ OPEval+")");
//				
//				// add the edge
//				newEdges[newEdgesMarker][0] = candidateEdgeV1;
//				newEdges[newEdgesMarker][1] = candidateEdgeV2;
//				newVals[newEdgesMarker]=OPEval;
//				newEdgesMarker++;
//			}
//			
//		}
//		newEdgesMain = newEdges;
//		newValsMain = newVals;
//	}

	public static boolean performComputation(int[][] gph, byte[] vals) {
		int newEdges[][] = new int[EDGE_BUFFER_SIZE][2];
		byte newVals[] = new byte[EDGE_BUFFER_SIZE];
		int newEdgesMarker = 0;

		// initializing newEdges
		for (int i = 0; i < newEdges.length; i++) {
			newEdges[i][0] = -1;
			newEdges[i][1] = -1;
			newVals[i] = -1;
		}
		
		boolean isNewEdgeAdded = false;
		int candidateEdgeV1, candidateEdgeV2;
		byte srcEval, destEval, OPEval;
		for (int j = 0; j < gph.length; j++) {
			candidateEdgeV1 = gph[j][0];
			candidateEdgeV2 = gph[j][1];
			srcEval = vals[j];
			
			OPEval = GrammarChecker.checkL1Rules(srcEval);
			
			if (OPEval!=-1) {
				
				boolean edgeExists = false;

				// check whether this edge already exists in the original graph
				edgeExists = isInGraph(gph, vals, candidateEdgeV1, candidateEdgeV2, OPEval, edgeExists);

				// check whether edge exists in the new edges
				edgeExists = isInGraph(newEdges, newVals, candidateEdgeV1, candidateEdgeV2, OPEval, edgeExists);
				
				if (!edgeExists) {
					
					// logger.info("New edge found: " + candidateEdgeV1 +  "---->" + candidateEdgeV2 + "("+ OPEval+")");
					
					// add the edge
					newEdges[newEdgesMarker][0] = candidateEdgeV1;
					newEdges[newEdgesMarker][1] = candidateEdgeV2;
					newVals[newEdgesMarker]=OPEval;
					newEdgesMarker++;
					isNewEdgeAdded = true;
				}
				
			}
			
			for (int k = 0; k < gph.length; k++) {
				if (gph[j][1] == gph[k][0] && gph[j][1] != -1 && gph[k][0] != -1) {
					candidateEdgeV1 = gph[j][0];
					candidateEdgeV2 = gph[k][1];
					
					srcEval=vals[j];
					destEval=vals[k];
					
					
					// check whether pair-wise grammar rule (d-rule) is satisfied
					OPEval = GrammarChecker.checkL2Rules(srcEval, destEval);
					
					logger.info("L2 Rule Check "+ OPEval+" "+srcEval+" "+destEval);
					
					if (OPEval==-1) continue;
					
					boolean edgeExists = false;

					// check whether this edge already exists in the original graph
					edgeExists = isInGraph(gph, vals, candidateEdgeV1, candidateEdgeV2, OPEval, edgeExists);

					// check whether edge exists in the new edges
					edgeExists = isInGraph(newEdges, newVals, candidateEdgeV1, candidateEdgeV2, OPEval, edgeExists);

					if (!edgeExists) {
						
						// logger.info("New edge found: " + candidateEdgeV1 +  "---->" + candidateEdgeV2 + "("+ OPEval+")");
						
						// add the edge
						newEdges[newEdgesMarker][0] = candidateEdgeV1;
						newEdges[newEdgesMarker][1] = candidateEdgeV2;
						newVals[newEdgesMarker]=OPEval;
						newEdgesMarker++;
						isNewEdgeAdded = true;
					}
				}
			}
		}
		
		logger.info("# new edges added in this iteration: " + newEdgesMarker);
		newEdgesMain = newEdges;
		newValsMain = newVals;
//		if (newEdgesMarker != 0) // if new edges have been added
//		{
//			printNewEdges();
//		}
		return isNewEdgeAdded;
	}
	
private static void storePart_ActualEdges(String basefilename) throws IOException {
		// clear current graph file
		PrintWriter partOutStrm = new PrintWriter(new BufferedWriter( new FileWriter(basefilename , false)));
		partOutStrm.close();

		partOutStrm = new PrintWriter(new BufferedWriter(new FileWriter(basefilename , true)));

		for (int i = 0; i < gph.length; i++) {
			if (gph[i][0] == -1)
				break;
			partOutStrm.println(gph[i][0] + "\t" + gph[i][1]+ "\t" + GrammarChecker.getValue((byte)vals[i]));
		}
		partOutStrm.close();
	}

	
	/**
	 * Checks whether an edge exists in a set of edges
	 * @param gph
	 * @param vals
	 * @param candidateEdgeV1
	 * @param candidateEdgeV2
	 * @param OPEval
	 * @param edgeExists
	 * @return
	 */
	private static boolean isInGraph(int[][] gph, byte[] vals, int candidateEdgeV1, int candidateEdgeV2, byte OPEval,boolean edgeExists) {
		if (edgeExists) {
			return true;
		}
		for (int m = 0; m < gph.length; m++) {
			if (candidateEdgeV1 == gph[m][0] && candidateEdgeV2 == gph[m][1] && OPEval==vals[m]) {
				// logger.info("Edge already exists: " +
				// candidateEdgeV1 + "---->" + candidateEdgeV2);
				edgeExists = true;
				break;
			}
		}
		return edgeExists;
	}

	/**
	 * Prints the whole graph
	 */
	public static void printGraph() {
		// input vertex
		int v = 8;

		String s = "";
		int numOfEdges = 0;
		for (int i = 0; i < gph.length; i++) {
			if (gph[i][0] == -1)
				break;
//			if (gph[i][0]==v){//only prints the graph for the input vertex
			s = s + gph[i][0] + "\t" + gph[i][1] + "\n";
//			numOfEdges++;
			}
//		}
//		logger.info("The complete graph showing only edges for vertex " + v
//				+ " is (" + numOfEdges + " edges): " + "\n" + s);
	}

	/**
	 * Prints the new edges
	 */
	public static void printNewEdges() {
		String s = "";
		for (int i = 0; i < newEdgesMain.length; i++) {
			if (newEdgesMain[i][0] == -1)
				break;
			s = s + newEdgesMain[i][0] + "\t" + newEdgesMain[i][1] + "\n";
		}
		logger.info("New edges added:" + "\n" + s);
	}
}
