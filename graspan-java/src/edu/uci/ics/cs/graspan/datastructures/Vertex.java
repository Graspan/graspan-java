package edu.uci.ics.cs.graspan.datastructures;

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
	
	private int[] outEdges;
	private byte[] outEdgeValues;

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
		this.numOutEdges = 0;
		this.outEdges = outEdges;
		this.outEdgeValues = outEdgeValues;
		if(outEdges != null){
			this.numOutEdges = outEdges.length;
//			assert(this.outEdges.length == this.outEdgeValues.length);
		}
	}

	public int getNumOutEdges() {
		this.numOutEdges = 0;
		
		if(this.outEdges != null){
			numOutEdges = outEdges.length;
		}
		return numOutEdges;
	}

	public void setNumOutEdges() {

	}

	/**
	 * Used by EdgeList Style Computation
	 * 
	 * @return
	 */
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

	public void setOutEdges(int outEdges[]) {
		this.outEdges = outEdges;
	}

	public void setOutEdgeValues(byte[] outEdgeValues) {
		this.outEdgeValues = outEdgeValues;
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

	public int getVertexIdx() {
		return idx;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");

		result.append(NEW_LINE + "vertex id : " + id + NEW_LINE);
		result.append("out edges: " + Arrays.toString(outEdges) + NEW_LINE);
		// result.append("edge value: " + Arrays.toString(outEdgeValues) +
		// NEW_LINE);//TODO UNCOMMENT THIS LATER
		result.append("degree: " + combinedDeg + NEW_LINE);

		return result.toString();
	}
}
