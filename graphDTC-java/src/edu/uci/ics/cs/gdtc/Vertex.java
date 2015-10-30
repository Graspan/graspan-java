package edu.uci.ics.cs.gdtc;

import java.util.Arrays;

/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class Vertex {
	private int id;
	private int numOutEdges;
	private int[] outEdges = null;
	private byte[] outEdgeValues = null;
	
	public Vertex(int id, int outDegree) {
		this.id = id;
		numOutEdges = outDegree;
		
		if(outDegree != 0) {
			outEdges = new int[outDegree];
			outEdgeValues = new byte[outDegree];
		}
	}
	
	public Vertex(int id, int[] outEdges, byte[] outEdgeValues) {
		this.id = id;
		this.numOutEdges = outEdges.length;
		this.outEdges = outEdges;
		this.outEdgeValues = outEdgeValues;
	}
	
	public int getNumOutEdges() {
		return numOutEdges;
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
		result.append("edge value: " + Arrays.toString(outEdgeValues) + NEW_LINE);
		
		return result.toString();
	}
}
