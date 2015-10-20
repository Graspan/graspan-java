package edu.uci.ics.cs.gdtc;

/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class GraphDTCVertex {
	private int id;
	private int numOutEdges;
	private int[] outEdges = null;
	private byte[] outEdgeValues = null;
	
	public GraphDTCVertex(int id, int outDegree) {
		this.id = id;
		numOutEdges = outDegree;
		
		if(outDegree != 0) {
			outEdges = new int[outDegree];
			outEdgeValues = new byte[outDegree];
		}
	}
	
	public GraphDTCVertex(int id, int[] outEdges, byte[] outEdgeValues) {
		this.id = id;
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
}
