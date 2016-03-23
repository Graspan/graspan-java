package edu.uci.ics.cs.graspan.datastructures;

/**
 * @author Kai Wang
 *
 *         Created Oct 28, 2015
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

	public void setIsNewEdgeAdded(boolean isNewEdgeAdded) {
		this.isNewEdgeAdded = isNewEdgeAdded;
	}

	public boolean hasNewEdges() {
		return isNewEdgeAdded;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");

		result.append(NEW_LINE + "(PartId: " + partitionId + ", VRange: " + firstVertex + "-" + lastVertex
				+ ", IdRange: " + indexStart + "-" + indexEnd + ")");

		return result.toString();
	}

}