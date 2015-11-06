package edu.uci.ics.cs.gdtc.engine;

/**
 * @author Kai Wang
 *
 * Created by Oct 28, 2015
 */
public class LoadedVertexInterval {
	private int firstVertex;
	private int lastVertex;
	private int indexStart;
	private int indexEnd;
	private int partitionId;
	private boolean isNewEdgeAdded;
	
	public LoadedVertexInterval(int firstVertex, int lastVertex, int partitionId) {
		this.firstVertex = firstVertex;
		this.lastVertex = lastVertex;
		this.partitionId = partitionId;
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
	
	public void setIsNewEdgeAdded(boolean isNewEdgeAdded) {
		this.isNewEdgeAdded = isNewEdgeAdded;
	}
	
	public boolean getIsNewEdgeAdded() {
		return isNewEdgeAdded;
	}
	
	public int getIndexEnd() {
		return indexEnd;
	}
	
	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}
	
	public int getPartitionId() {
		return partitionId;
	}
}
