package edu.uci.ics.cs.gdtc.edgecomputation;

import edu.uci.ics.cs.gdtc.GraphDTCVertex;


/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class EdgeComputation {
	
	public static void execUpate(GraphDTCVertex vertex, GraphDTCVertex[] verticesFrom, 
			GraphDTCVertex[] verticesTo, GraphDTCNewEdgesList edgeList, GraphDTCNewEdgesList[] edgesLists) {
		if(vertex == null || verticesFrom == null || verticesTo == null 
				|| edgeList == null || edgesLists == null)
			return;
		int nOutEdges = vertex.getNumOutEdges();
		if(nOutEdges == 0) return;
		
		// 1. check edges which are stored in vertex outEdges array
		for(int i = 0; i < nOutEdges; i++) {
			
			// get dst vertex id and edge value from array
			int vertexId = vertex.getOutEdge(i);
			byte edgeValue = vertex.getOutEdgeValue(i);
			
			// scan edges in partition "from"
			if(isInRange(vertexId, verticesFrom))
				scanAddableEdges(vertexId, edgeValue, verticesFrom, 
						vertex, edgeList, edgesLists);
			
			// scan edges in partition "to"
			if(isInRange(vertexId, verticesTo))
				scanAddableEdges(vertexId, edgeValue, verticesTo, 
						vertex, edgeList, edgesLists);
			
		}
		
	}

	/**
	 * Description:
	 * @param:
	 * @return:boolean
	 */
	// TODO: to be optimized
	private static boolean isInRange(int vertexId, GraphDTCVertex[] vertices) {
		if(vertices == null || vertices.length == 0)
				return false;
		
		int len = vertices.length;
		int intervalSt = vertices[0].getVertexId();
		int intervalEnd = vertices[len - 1].getVertexId();
		if(vertexId >= intervalSt && vertexId <= intervalEnd)
			return true;
		
		return false;
	}

	/**
	 * Description:
	 * @param vertex 
	 * @param edgeValue 
	 * @param vertexId 
	 * @param:
	 * @return:
	 */
	private static boolean isDuplicationEdge(int vertexId, byte edgeValue, 
			GraphDTCVertex vertex, GraphDTCNewEdgesList edgeList) {
		if(vertex == null || vertex.getNumOutEdges() == 0)
			return false;
		
		// 1. check fixed size array
		for(int i = 0; i < vertex.getNumOutEdges(); i++) {
			int id = vertex.getOutEdge(i);
			byte value = vertex.getOutEdgeValue(i);
			if((vertexId == id) && (edgeValue == value))
				return true;
		}
		
		// 2. check new edges linked array
		if(edgeList == null)
			return false;
		
		int size = edgeList.getSize();
		if(size == 0)
			return false;
		
		int[] ids = null;
		byte[] values = null;
		
		// 2.1 check (size - 1) node, each node is full of NODE_SIZE elements
		for(int j = 0; j < size - 1; j++) {
			ids = edgeList.getNode(j).getDstVertices();
			values = edgeList.getNode(j).getEdgeValues();
			assert(ids.length == GraphDTCNewEdgesList.NODE_SIZE);
			
			for(int k = 0; k < ids.length; k++) {
				if((ids[k] == vertexId) && (values[k] == edgeValue))
					return true;
			}
		}
		
		// 2.2 check the last node, the num of elements is index
		ids = edgeList.getLast().getDstVertices();
		values = edgeList.getLast().getEdgeValues();
		int index = edgeList.getIndex();
		for(int m = 0; m < index; m++) {
			if((ids[m] == vertexId) && (values[m] == edgeValue))
				return true;
		}
		
		return false;
	}

	/**
	 * Description:
	 * @param vertices
	 * @param vertexId 
	 * @param:
	 * @return:
	 */
	private static void scanAddableEdges(int vertexId, byte edgeValue, GraphDTCVertex[] vertices, 
			GraphDTCVertex vertex, GraphDTCNewEdgesList edgeList, GraphDTCNewEdgesList[] edgesLists) {
		if(vertices == null || vertices.length == 0)
			return;
		
		int vertexSt = vertices[0].getVertexId();
		int index = vertexId - vertexSt;
		GraphDTCVertex v = vertices[index];
		if(v == null || v.getNumOutEdges() == 0)
			return;
		
		final Object lock = new Object();
		
		// 1. scan original fixed size array
		for(int i = 0; i < v.getNumOutEdges(); i++) {
			int dstId = v.getOutEdge(i);
			byte dstEdgeValue = v.getOutEdgeValue(i);
			
			//TODO: add grammar check
			if(checkGrammar()) {
				//TODO: dstEdgeValue to be fixed based on grammar!!
				if(!isDuplicationEdge(dstId, dstEdgeValue, vertex, edgeList)) {
					// no duplication, needs to add edge to linked array
					// synchronize, to guarantee happens-before relationship
					synchronized (lock) {
						edgeList.add(dstId, dstEdgeValue);
					}
				}
			}
		}
		
		// 2. scan new edges linked array
		if(edgeList == null) return;
		
		int readableSize = edgesLists[index].getReadableSize();
		if(readableSize == 0) return;
		
		int[] ids = null;
		byte[] values = null;
		
		// 2.1 check (readableSize - 1) node, each node is full of NODE_SIZE elements
		for(int j = 0; j < readableSize - 1; j++) {
			ids = edgesLists[index].getNode(j).getDstVertices();
			values = edgesLists[index].getNode(j).getEdgeValues();
			assert(ids.length == GraphDTCNewEdgesList.NODE_SIZE);
			
			for(int k = 0; k < ids.length; k++) {
				//TODO: add grammar check
				if(checkGrammar()) {
					//TODO: dstEdgeValue to be fixed based on grammar!!
					if(!isDuplicationEdge(ids[k], values[k], vertex, edgeList)) {
						// no duplication, needs to add edge to linked array
						// synchronize, to guarantee happens-before relationship
						synchronized (lock) {
							edgeList.add(ids[k], values[k]);
						}
					}
				}
			}
		}
		
		// 2.2 check the last node, the num of elements is index
		ids = edgesLists[index].getLast().getDstVertices();
		values = edgesLists[index].getLast().getEdgeValues();
		int readableIndex = edgesLists[index].getReadableIndex();
		for(int m = 0; m < readableIndex; m++) {
			
			//TODO: add grammar check
			if(checkGrammar()) {
				//TODO: dstEdgeValue to be fixed based on grammar!!
				if(!isDuplicationEdge(ids[m], values[m], vertex, edgeList)) {
					// no duplication, needs to add edge to linked array
					// synchronize, to guarantee happens-before relationship
					synchronized (lock) {
						edgeList.add(ids[m], values[m]);
					}
				}
			}
		}
	}

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private static boolean checkGrammar() {
		return true;
	}
}
