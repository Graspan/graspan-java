package edu.uci.ics.cs.gdtc.partitiondata;

/**
 * @author Kai Wang
 *
 *         Created by Oct 28, 2015
 */
public class LoadedVertexInterval {
	private int firstVertex;
	private int lastVertex;
	private int indexStart;
	private int indexEnd;
	private int partitionId;
	private boolean saved;

	public LoadedVertexInterval(int firstVertex, int lastVertex, int partitionId) {
		this.firstVertex = firstVertex;
		this.lastVertex = lastVertex;
		this.partitionId = partitionId;
	}

	public void setFirstVertex(int firstVertex) {
		this.firstVertex = firstVertex;
	}

	/**
	 * 
	 * @return int firstVertex
	 */
	public int getFirstVertex() {
		return firstVertex;
	}

	public void setLastVertex(int lastVertex) {
		this.lastVertex = lastVertex;
	}

	/**
	 * 
	 * @return int lastVertex
	 */
	public int getLastVertex() {
		return lastVertex;
	}

	public void setIndexStart(int indexStart) {
		this.indexStart = indexStart;
	}

	/**
	 * 
	 * @return int indexStart
	 */
	public int getIndexStart() {
		return indexStart;
	}

	public void setIndexEnd(int indexEnd) {
		this.indexEnd = indexEnd;
	}

	/**
	 * 
	 * @return int indexEnd
	 */
	public int getIndexEnd() {
		return indexEnd;
	}

	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	/**
	 * 
	 * @return int partitionId
	 */
	public int getPartitionId() {
		return partitionId;
	}

	/**
	 * 
	 * @param val
	 */
	public void setSavedFlag(boolean val) {
		this.saved = val;
	}

	/**
	 * 
	 * @return
	 */
	public boolean getSavedFlag() {
		return this.saved;
	}
}