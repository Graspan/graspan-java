package edu.uci.ics.gdtc;

/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class GraphDTCNewEdgesList {
	private static final int NODE_SIZE = 256;
	
	private NewEdgesNode first;
	private NewEdgesNode last;
	private int size;
	private int readableSize;
	private int readableIndex;
	
	class NewEdgesNode {
		private int[] dstVertices = null;
		private char[] edgeValues = null;
		private int index;
		private NewEdgesNode next = null;
		
		NewEdgesNode() {
			dstVertices = new int[NODE_SIZE];
			edgeValues = new char[NODE_SIZE];
			
		}
		
		public int[] getDstVertices() {
			return dstVertices;
		}
		
		public char[] getEdgeValues() {
			return edgeValues;
		}
		
		public int getIndex() {
			return index;
		}
		
		public void add(int vertexId, char edgeValue) {
			dstVertices[index] = vertexId;
			edgeValues[index] = edgeValue;
			index++;
		}
		
	}
	
	public int getSize() {
		return size;
	}
	
	public int getReadableSize() {
		return readableSize;
	}
	
	public int getReadableIndex() {
		return readableIndex;
	}
	
	public void setReadableSize(int readableSize) {
		this.readableSize = readableSize;
	}
	
	public void setReadableIndex(int readableIndex) {
		this.readableIndex = readableIndex;
	}
	
	public NewEdgesNode getFirst() {
		return first;
	}
	
	public NewEdgesNode getLast() {
		return last;
	}
	
	public int getIndex() {
		return last.getIndex();
	}
	
	public void add(int vertexId, char edgeValue) {
		if(size == 0) {
			first = new NewEdgesNode();
			last = first;
			
		} else {
			int index = last.getIndex();
			if(index >= NODE_SIZE) {
				last.next = new NewEdgesNode();
				last = last.next;
			}
			
		}
		
		size++;
		last.add(vertexId, edgeValue);
	}
}
