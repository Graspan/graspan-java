package edu.uci.ics.graspan.oracle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.GrammarChecker;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * Array-based Graph Extractor
 * @author aftab
 *
 */
public class GraphExtractor {
	
	private static final Logger logger = GraspanLogger.getLogger("GraphExtractor");
	
	//NOTE: MAKE SURE RANGE IS BETWEEN THE LIMITS OF THE GRAPH
	private static final int RANGE_START =45000;
	private static final int RANGE_END =55000;
	private static final int EDGE_BUFFER_SIZE = RANGE_END - RANGE_START + 1;
	
	private static String basefilename;
	
	public static int gph[][] = new int[EDGE_BUFFER_SIZE][2];
	public static byte vals[]=new byte[EDGE_BUFFER_SIZE];

	public static void main(String args[]) throws NumberFormatException, IOException {

		init(args);
		
		// read in the original graph
		readGraph();
		logger.info("Read graph");
		storeGraph();
		logger.info("Stored graph");

	}
	
	private static String init(String[] args) {
		basefilename = args[0];
		logger.info("Input graph: " + args[0]);
		
		// initialize the graph data structures with -1
		for (int i = 0; i < gph.length; i++) {
			gph[i][0] = -1;
			gph[i][1] = -1;
			vals[i]=-1;
		}
		logger.info("Completed initialization of graph data structures.");
		return basefilename;
	}
	

	private static void readGraph() throws FileNotFoundException, IOException {
		BufferedReader ins = new BufferedReader(new InputStreamReader(new FileInputStream(new File(basefilename))));
		String ln;
		int gphPtr = 0, linePtr = 0;
		while ((ln = ins.readLine()) != null) {
			if (!ln.isEmpty()) {
				linePtr++;
				if (linePtr >= RANGE_START & linePtr <= RANGE_END) {
					logger.info(linePtr+"");
					String[] tok = ln.split("\t");// NOTE: MAKE SURE INPUT FILEIS TAB DELIMITED
					gph[gphPtr][0] = Integer.parseInt(tok[0]);
					gph[gphPtr][1] = Integer.parseInt(tok[1]);
					vals[gphPtr] = GrammarChecker.getValue(tok[2].trim());
					gphPtr++;
				}
			}
		}
	}
	
	private static void storeGraph() throws IOException {
		// clear current graph file
//		PrintWriter partOutStrm = new PrintWriter(new BufferedWriter( new FileWriter(basefilename , false)));
//		partOutStrm.close();

		PrintWriter partOutStrm = new PrintWriter(new BufferedWriter(new FileWriter(basefilename+".extracted" , true)));

		for (int i = 0; i < gph.length; i++) {
			if (gph[i][0] == -1)
				break;
			partOutStrm.println(gph[i][0] + "\t" + gph[i][1]+ "\t" + GrammarChecker.getValue((byte)vals[i]));
		}
		partOutStrm.close();
	}
	
	
}
