package edu.uci.ics.cs.gdtc.partitiondata;

import java.util.Arrays;

/**
 * @author Kai Wang
 *
 *         Created by Oct 8, 2015
 */
public class Vertex {

	// index of the vertex in Vertex[] Vertices array
	private int idx;

	private int id;
	private int numOutEdges;
	private int[] outEdges = null;
	private byte[] outEdgeValues = null;

	// the degree of the vertex counting original out edges and newly computed
	// out edges
	private int combinedDeg;

	// Unused
	public Vertex(int id, int outDegree) {
		this.id = id;
		numOutEdges = outDegree;

		if (outDegree != 0) {
			outEdges = new int[outDegree];
			outEdgeValues = new byte[outDegree];
		}
	}

	public Vertex(int idx, int id, int[] outEdges, byte[] outEdgeValues) {
		this.idx = idx;
		this.id = id;
		this.numOutEdges = outEdges.length;
		this.outEdges = outEdges;
		this.outEdgeValues = outEdgeValues;
	}

	public int getNumOutEdges() {
		return numOutEdges;
	}

	public int getCombinedDeg() {
		return combinedDeg;
	}

	public void setCombinedDeg(int num) {
		combinedDeg = num;
	}

	public int[] getOutEdges() {
		return outEdges;
	}

	public byte[] getOutEdgeValues() {
		return outEdgeValues;
	}

	public int getOutEdge(int i) {
		return outEdges[i];
	}

	public byte getOutEdgeValue(int i) {
		return outEdgeValues[i];
	}

	public int getVertexId() {
		return id;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");

		result.append(NEW_LINE + "vertex id : " + id + NEW_LINE);
		result.append("out edges: " + Arrays.toString(outEdges) + NEW_LINE);
		// result.append("edge value: " + Arrays.toString(outEdgeValues) +
		// NEW_LINE);//TODO UNCOMMENT THIS LATER

		return result.toString();
	}
}
