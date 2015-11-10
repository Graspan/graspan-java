package edu.uci.ics.cs.gdtc.edgecomputer;

import java.util.Arrays;

/**
 * @author Kai Wang
 *
 *         Created by Oct 8, 2015
 */
public class NewEdgesList {
	public static final int NODE_SIZE = 10;

	private NewEdgesNode first;
	private NewEdgesNode last;
	private int size;
	private int readableSize;
	private int readableIndex;

	public class NewEdgesNode {
		private int[] dstVertices = null;
		private byte[] edgeValues = null;
		private int index;
		private NewEdgesNode next = null;

		NewEdgesNode() {
			dstVertices = new int[NODE_SIZE];
			edgeValues = new byte[NODE_SIZE];

			for (int i = 0; i < NODE_SIZE; i++) {
				dstVertices[i] = -1;
			}

		}

		public int[] getDstVertices() {
			return dstVertices;
		}

		public byte[] getEdgeValues() {
			return edgeValues;
		}

		public int getIndex() {
			return index;
		}

		public void add(int vertexId, byte edgeValue) {
			dstVertices[index] = vertexId;
			edgeValues[index] = edgeValue;
			index++;
		}

	}

	public NewEdgesList() {
		last = first = null;
		size = 0;
		readableIndex = 0;
		readableSize = 0;
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

	public void add(int vertexId, byte edgeValue) {
		if (size == 0) {
			first = new NewEdgesNode();
			size++;
			last = first;

		} else {
			int index = last.getIndex();
			if (index >= NODE_SIZE) {
				last.next = new NewEdgesNode();
				last = last.next;
				size++;
			}

		}

		last.add(vertexId, edgeValue);
	}

	public NewEdgesNode getNode(int index) {
		if (index == 0)
			return first;
		if (index > size)
			return null;

		NewEdgesNode node = first;
		for (int i = 0; i < index; i++) {
			node = node.next;
		}

		return node;
	}

	@Override
	public String toString() {
		if (size == 0)
			return "[]";

		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");
		NewEdgesNode temp = first;
		for (int i = 0; i < size; i++) {
			result.append(NEW_LINE + "edge list size : " + size);
			result.append(NEW_LINE + "dst vertex id : " + Arrays.toString(temp.getDstVertices()));
			// result.append(NEW_LINE + "edge value : " +
			// Arrays.toString(temp.getEdgeValues()));//TODO UNCOMMENT THIS
			// LATER
			temp = temp.next;
		}

		return result.toString();
	}
}
