package edu.uci.ics.cs.artificalgraphgenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

/**
 * This program adds random edge values to a given input graph
 * 
 * @author Aftab
 *
 */
public class EdgeValueAdder {

	static final int BUFFER = 1000000000;
	static int BUFFER_FREE_SPACE = BUFFER;
	static ArrayList<String> EDGE_BUFFER;

	public static void main(String args[]) throws IOException {

		EDGE_BUFFER = new ArrayList<String>();
		long linecount = 0;

		Random rand = new Random();
		String baseFilename = args[0];
		InputStream a = new FileInputStream(new File(baseFilename));
		BufferedReader ins = new BufferedReader(new InputStreamReader(a));
		String ln;
		String modifiedInput;

		PrintWriter modifedEdgefile = new PrintWriter(
				new BufferedWriter(new FileWriter(baseFilename + ".edgevaladded.", true)));
		while ((ln = ins.readLine()) != null) {

			linecount++;
			if (linecount % 10000000 == 0) {
				System.out.println("Reading Line " + linecount);
			}
			modifiedInput = ln + "\t" + rand.nextInt(256) + "\n";
			addEdgetoBuffer(modifiedInput);
			if (BUFFER_FREE_SPACE == 0) {
				writeToDisk(modifedEdgefile, baseFilename);
			}
			// add remaining edges in buffer to disk
			writeToDisk(modifedEdgefile, baseFilename);
		}
		ins.close();
		modifedEdgefile.close();
	}

	private static void addEdgetoBuffer(String modifiedInput) throws IOException {
		EDGE_BUFFER.add(modifiedInput);
		BUFFER_FREE_SPACE--;
	}

	public static void writeToDisk(PrintWriter modifedEdgefile, String baseFilename) throws IOException {

		for (int i = 0; i < EDGE_BUFFER.size(); i++) {
			modifedEdgefile.write(EDGE_BUFFER.get(i));
		}
		EDGE_BUFFER.clear();
		BUFFER_FREE_SPACE = BUFFER;
	}

}
