package edu.uci.ics.cs.graspan.computation;

import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.NewEdgesList;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * @author Kai Wang
 *
 *         Created by Oct 8, 2015
 */
public class EdgeComputer {
	
	private static final Logger logger = GraspanLogger.getLogger("EdgeComputer");

	// test flag for viewing new edges of a source
	public static int flag = 0;
	String s = "";

	private Vertex vertex = null;
	private NewEdgesList edgeList = null;
	private int nNewEdges;
	private boolean terminateStatus;
	private static NewEdgesList[] edgesLists = null;
	private static Vertex[] vertices = null;
	private static List<LoadedVertexInterval> intervals = null;

	public EdgeComputer(Vertex vertex, NewEdgesList edgeList) {
		this.vertex = vertex;
		this.edgeList = edgeList;
	}

	public int getNumNewEdges() {
		return nNewEdges;
	}

	public void setNumNewEdges(int nNewEdges) {
		this.nNewEdges = nNewEdges;
	}

	public boolean getTerminateStatus() {
		return terminateStatus;
	}

	public void setTerminateStatus(boolean terminateStatus) {
		this.terminateStatus = terminateStatus;
	}

	public static void setEdgesLists(NewEdgesList[] lists) {
		edgesLists = lists;
	}

	public static void setVertices(Vertex[] v) {
		vertices = v;
	}

	public static void setIntervals(List<LoadedVertexInterval> vertexIntervals) {
		intervals = vertexIntervals;
	}

	public void execUpdate() {
		if (vertex == null || vertices == null)
			return;

		int nOutEdges = vertex.getNumOutEdges();
		if (nOutEdges == 0)
			return;

		// get edges of vertex 2 / test code 1
		if (vertex.getVertexId() == 2)
			flag = 1;

		// 1. check edges which are stored in vertex outEdges array
		for (int i = 0; i < nOutEdges; i++) {

			// get dst vertex id and edge value from array
			int vertexId = vertex.getOutEdge(i);
			byte edgeValue = vertex.getOutEdgeValue(i);

			// get edges of vertex 2 / test code 2
			// if (flag == 1) {
			// s = 2 + " ---> " + vertexId;
			// System.out.println(s);
			// }

			// check vertex range and scan edges
			checkRangeAndScanEdges(vertexId, edgeValue);

		}

		// 2. check new edges linked array
		if (edgeList == null)
			return;
		int readableSize = edgeList.getReadableSize();
		if (readableSize == 0)
			return;

		int[] ids = null;
		byte[] values = null;

		// 2.1 check (readableSize - 1) node, each node is full of NODE_SIZE
		// elements
		for (int j = 0; j < readableSize - 1; j++) {
			ids = edgeList.getNode(j).getDstVertices();
			values = edgeList.getNode(j).getEdgeValues();
			assert(ids.length == NewEdgesList.NODE_SIZE);

			for (int k = 0; k < ids.length; k++) {

				// check vertex range and scan edges
				checkRangeAndScanEdges(ids[k], values[k]);

			}
		}

		// 2.2 check the last node, the num of elements is index
		ids = edgeList.getReadableLast().getDstVertices();
		values = edgeList.getReadableLast().getEdgeValues();
		int readableIndex = edgeList.getReadableIndex();
		for (int m = 0; m < readableIndex; m++) {

			// check vertex range and scan edges
			checkRangeAndScanEdges(ids[m], values[m]);

		}
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return: boolean
	 */
	// TODO: to be optimized
	private boolean isInRange(int vertexId) {
		for (LoadedVertexInterval interval : intervals) {
			int intervalSt = interval.getFirstVertex();
			int intervalEnd = interval.getLastVertex();
			if (vertexId >= intervalSt && vertexId <= intervalEnd)
				return true;
		}

		return false;
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private void checkRangeAndScanEdges(int vertexId, byte edgeValue) {
		if (isInRange(vertexId))
			scanEdges(vertexId, edgeValue);
	}

	/**
	 * Description:
	 * 
	 * @param vertex
	 * @param edgeValue
	 * @param vertexId
	 * @param:
	 * @return:
	 */
	private boolean isDuplicationEdge(int vertexId, byte edgeValue) {
		if (vertex == null || vertex.getNumOutEdges() == 0)
			return false;

		// 1. check fixed size array
		for (int i = 0; i < vertex.getNumOutEdges(); i++) {
			int id = vertex.getOutEdge(i);
			byte value = vertex.getOutEdgeValue(i);
			// if((vertexId == id) && (edgeValue == value))
			// TODO: JUST for testing, change back soon!!!
			if (vertexId == id)
				return true;
		}

		// 2. check new edges linked array
		if (edgeList == null)
			return false;

		int size = edgeList.getSize();
		if (size == 0)
			return false;

		int[] ids = null;
		byte[] values = null;

		// 2.1 check (size - 1) node, each node is full of NODE_SIZE elements
		for (int j = 0; j < size - 1; j++) {
			ids = edgeList.getNode(j).getDstVertices();
			values = edgeList.getNode(j).getEdgeValues();
			assert(ids.length == NewEdgesList.NODE_SIZE);

			for (int k = 0; k < ids.length; k++) {
				// TODO: JUST for testing, change back soon!!!
				// if((ids[k] == vertexId) && (values[k] == edgeValue))
				if (ids[k] == vertexId)
					return true;
			}
		}

		// 2.2 check the last node, the num of elements is index
		ids = edgeList.getLast().getDstVertices();
		values = edgeList.getLast().getEdgeValues();
		int index = edgeList.getIndex();
		for (int m = 0; m < index; m++) {
			// TODO: JUST for testing, change back soon!!!
			// if((ids[m] == vertexId) && (values[m] == edgeValue))
			if (ids[m] == vertexId)
				return true;
		}

		return false;
	}

	private int getDstVertexIndex(int vertexId) {
		for (LoadedVertexInterval interval : intervals) {
			int intervalSt = interval.getFirstVertex();
			int intervalEnd = interval.getLastVertex();
			if ((vertexId >= intervalSt) && (vertexId <= intervalEnd)) {
				int indexSt = interval.getIndexStart();
				return (indexSt + vertexId - intervalSt);
			}
		}
		return -1;
	}

	/**
	 * Description:
	 * 
	 * @param vertices
	 * @param vertexId
	 * @param:
	 * @return:
	 */
	private void scanEdges(int vertexId, byte edgeValue) {
		if (vertices == null || vertices.length == 0)
			return;

		int index = getDstVertexIndex(vertexId);
		if (index == -1)
			return;

		assert index >= 0 && index < vertices.length;
		Vertex v = vertices[index];
		if (v == null || v.getNumOutEdges() == 0)
			return;

		// 1. scan original fixed size array
		for (int i = 0; i < v.getNumOutEdges(); i++) {
			int dstId = v.getOutEdge(i);
			byte dstEdgeValue = v.getOutEdgeValue(i);

			// get edges of vertex 2 / test code 3
			// if (flag == 1) {
			// String olds = s;
			// s = s + " ---> " + dstId;
			// System.out.println(s);
			// s = olds;
			// }

			// check grammar, check duplication and add edges
			// TODO: dstEdgeValue to be fixed based on grammar!!
			checkGrammarAndAddEdge(dstId, dstEdgeValue);
		}

		// 2. scan new edges linked array
		if (edgeList == null)
			return;

		NewEdgesList dstNewEdgeList = edgesLists[index];
		if (dstNewEdgeList == null)
			return;

		int readableSize = dstNewEdgeList.getReadableSize();
		if (readableSize == 0)
			return;

		int[] ids = null;
		byte[] values = null;

		// 2.1 check (readableSize - 1) node, each node is full of NODE_SIZE
		// elements
		for (int j = 0; j < readableSize - 1; j++) {
			ids = edgesLists[index].getNode(j).getDstVertices();
			values = edgesLists[index].getNode(j).getEdgeValues();
			assert(ids.length == NewEdgesList.NODE_SIZE);

			for (int k = 0; k < ids.length; k++) {
				// test
				// if (flag == 1) {
				// String olds = s;
				// s = s + " ---> " + ids[k];
				// System.out.println(s);
				// s = olds;
				// }
				// check grammar, check duplication and add edges
				// TODO: values[k] to be fixed based on grammar!!
				checkGrammarAndAddEdge(ids[k], values[k]);

			}
		}

		// 2.2 check the last node, the num of elements is index
		ids = edgesLists[index].getReadableLast().getDstVertices();
		values = edgesLists[index].getReadableLast().getEdgeValues();
		int readableIndex = edgesLists[index].getReadableIndex();
		for (int m = 0; m < readableIndex; m++) {
			// check grammar, check duplication and add edges
			// TODO: values[m] to be fixed based on grammar!!
			checkGrammarAndAddEdge(ids[m], values[m]);
		}
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private void checkGrammarAndAddEdge(int vertexId, byte edgeValue) {

		// TODO: add grammar check
		if (checkGrammar()) {
			// TODO: dstEdgeValue to be fixed based on grammar!!
			if (!isDuplicationEdge(vertexId, edgeValue)) {
				// no duplication, needs to add edge to linked array
				// TODO: assume happens-before relationship is guaranteed
				// by main thread waiting for all threads finish
				edgeList.add(vertexId, edgeValue);

				// get edges of vertex 2 / test code 4
				// if (flag == 1) {
				// System.out.println("edge added " + vertexId);
				// }

				nNewEdges++;
			}
		}
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private static boolean checkGrammar() {
		return true;
	}
}