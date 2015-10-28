package edu.uci.ics.cs.gdtc.engine;

/**
 * @author Kai Wang
 *
 * Created by Oct 28, 2015
 */
public class VertexInterval {
	private int firstVertex;
	private int lastVertex;
	private int indexStart;
	private int indexEnd;
	
	public VertexInterval(int firstVertex, int lastVertex) {
		this.firstVertex = firstVertex;
		this.lastVertex = lastVertex;
	}
	
	public void setFirstVertex(int firstVertex) {
		this.firstVertex = firstVertex;
	}
	
	public int getFirstVertex() {
		return firstVertex;
	}
	
	public void setLastVertex(int lastVertex) {
		this.lastVertex = lastVertex;
	}
	
	public int getLastVertex() {
		return lastVertex;
	}
	
	public void setIndexStart(int indexStart) {
		this.indexStart = indexStart;
	}
	
	public int getIndexStart() {
		return indexStart;
	}
	
	public void setIndexEnd(int indexEnd) {
		this.indexEnd = indexEnd;
	}
	
	public int getIndexEnd() {
		return indexEnd;
	}
}
