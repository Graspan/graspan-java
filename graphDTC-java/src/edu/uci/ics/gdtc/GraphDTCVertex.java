package edu.uci.ics.gdtc;

/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class GraphDTCVertex {
	private int id;
	private int numOutEdges;
	private int[] outEdges = null;
	private char[] outEdgeValues = null;
	
	public GraphDTCVertex(int id, int outDegree) {
		this.id = id;
		numOutEdges = outDegree;
		
		if(outDegree != 0) {
			outEdges = new int[outDegree];
			outEdgeValues = new char[outDegree];
		}
	}
	
	public int getNumOutEdges() {
		return numOutEdges;
	}
	
	public int[] getOutEdges() {
		return outEdges;
	}
	
	public char[] getOutEdgeValues() {
		return outEdgeValues;
	}
	
	public int getOutEdge(int i) {
		return outEdges[i];
	}
	
	public char getOutEdgeValue(int i) {
		return outEdgeValues[i];
	}
}
