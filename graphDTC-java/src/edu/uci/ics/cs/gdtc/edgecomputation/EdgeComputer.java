package edu.uci.ics.cs.gdtc.edgecomputation;

import edu.uci.ics.cs.gdtc.GraphDTCVertex;


/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class EdgeComputer {
	private GraphDTCVertex vertex = null;
	private GraphDTCNewEdgesList edgeList = null;
	private int nNewEdges;
	private boolean terminateStatus;
	private static GraphDTCNewEdgesList[] edgesLists= null;
	private static GraphDTCVertex[] verticesFrom = null;
	private static GraphDTCVertex[] verticesTo = null;
	
	public EdgeComputer(GraphDTCVertex vertex, GraphDTCNewEdgesList edgeList) {
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
	
	public static void setEdgesLists(GraphDTCNewEdgesList[] lists) {
		edgesLists = lists;
	}
	
	public static void setVerticesFrom(GraphDTCVertex[] vertices) {
		verticesFrom = vertices;
	}
	
	public static void setVerticesTo(GraphDTCVertex[] vertices) {
		verticesTo = vertices;
	}
	
	public void execUpdate() {
		if(vertex == null || verticesFrom == null || verticesTo == null)
			return;
		
		int nOutEdges = vertex.getNumOutEdges();
		if(nOutEdges == 0) return;
		
		// 1. check edges which are stored in vertex outEdges array
		for(int i = 0; i < nOutEdges; i++) {
			
			// get dst vertex id and edge value from array
			int vertexId = vertex.getOutEdge(i);
			byte edgeValue = vertex.getOutEdgeValue(i);
			
			// check vertex range and scan edges
			checkRangeAndScanEdges(vertexId, edgeValue);
			
		}
		
		// 2. check new edges linked array
		if(edgeList == null) return;
		int readableSize = edgeList.getReadableSize();
		if(readableSize == 0) return;
		
		int[] ids = null;
		byte[] values = null;
		
		// 2.1 check (readableSize - 1) node, each node is full of NODE_SIZE elements
		for(int j = 0; j < readableSize - 1; j++) {
			ids = edgeList.getNode(j).getDstVertices();
			values = edgeList.getNode(j).getEdgeValues();
			assert(ids.length == GraphDTCNewEdgesList.NODE_SIZE);
			
			for(int k = 0; k < ids.length; k++) {
				
				// check vertex range and scan edges
				checkRangeAndScanEdges(ids[k], values[k]);
				
			}
		}
		
		// 2.2 check the last node, the num of elements is index
		ids = edgeList.getLast().getDstVertices();
		values = edgeList.getLast().getEdgeValues();
		int readableIndex = edgeList.getReadableIndex();
		for(int m = 0; m < readableIndex; m++) {
			
			// check vertex range and scan edges
			checkRangeAndScanEdges(ids[m], values[m]);
			
		}
	}

	/**
	 * Description:
	 * @param:
	 * @return: boolean
	 */
	// TODO: to be optimized
	private boolean isInRange(int vertexId, GraphDTCVertex[] vertices) {
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
	 * @param:
	 * @return: 
	 */
	private void checkRangeAndScanEdges(int vertexId, byte edgeValue) {
		
		// scan edges in partition "from"
		if(isInRange(vertexId, verticesFrom))
			scanEdges(vertexId, edgeValue, verticesFrom);
					
		// scan edges in partition "to"
//		if(isInRange(vertexId, verticesTo))
//			scanEdges(vertexId, edgeValue, verticesTo);
	}
			

	/**
	 * Description:
	 * @param vertex 
	 * @param edgeValue 
	 * @param vertexId 
	 * @param:
	 * @return:
	 */
	private boolean isDuplicationEdge(int vertexId, byte edgeValue) {
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
		if(edgeList == null)	return false;
		
		int readableSize = edgeList.getReadableSize();
		if(readableSize == 0)	return false;
		
		int[] ids = null;
		byte[] values = null;
		
		// 2.1 check (size - 1) node, each node is full of NODE_SIZE elements
		for(int j = 0; j < readableSize - 1; j++) {
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
	private void scanEdges(int vertexId, byte edgeValue, GraphDTCVertex[] vertices) {
		if(vertices == null || vertices.length == 0)
			return;
		
		int vertexSt = vertices[0].getVertexId();
		int index = vertexId - vertexSt;
		GraphDTCVertex v = vertices[index];
		if(v == null || v.getNumOutEdges() == 0)
			return;
		
//		final Object lock = new Object();
		
		// 1. scan original fixed size array
		for(int i = 0; i < v.getNumOutEdges(); i++) {
			int dstId = v.getOutEdge(i);
			byte dstEdgeValue = v.getOutEdgeValue(i);
			
			// check grammar, check duplication and add edges
			//TODO: dstEdgeValue to be fixed based on grammar!!
			checkGrammarAndAddEdge(dstId, dstEdgeValue);
		}
		
		// 2. scan new edges linked array
		if(edgeList == null) return;
		
		GraphDTCNewEdgesList dstNewEdgeList = edgesLists[index];
		if(dstNewEdgeList == null) return;
		
		int readableSize = dstNewEdgeList.getReadableSize();
		if(readableSize == 0) return;
		
		int[] ids = null;
		byte[] values = null;
		
		// 2.1 check (readableSize - 1) node, each node is full of NODE_SIZE elements
		for(int j = 0; j < readableSize - 1; j++) {
			ids = edgesLists[index].getNode(j).getDstVertices();
			values = edgesLists[index].getNode(j).getEdgeValues();
			assert(ids.length == GraphDTCNewEdgesList.NODE_SIZE);
			
			for(int k = 0; k < ids.length; k++) {
				// check grammar, check duplication and add edges
				//TODO: values[k] to be fixed based on grammar!!
				checkGrammarAndAddEdge(ids[k], values[k]);
				
			}
		}
		
		// 2.2 check the last node, the num of elements is index
		ids = edgesLists[index].getLast().getDstVertices();
		values = edgesLists[index].getLast().getEdgeValues();
		int readableIndex = edgesLists[index].getReadableIndex();
		for(int m = 0; m < readableIndex; m++) {
			// check grammar, check duplication and add edges
			//TODO: values[m] to be fixed based on grammar!!
			checkGrammarAndAddEdge(ids[m], values[m]);
		}
	}
	
	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void checkGrammarAndAddEdge(int vertexId, byte edgeValue) {
		
		//TODO: add grammar check
		if(checkGrammar()) {
			//TODO: dstEdgeValue to be fixed based on grammar!!
			if(!isDuplicationEdge(vertexId, edgeValue)) {
				// no duplication, needs to add edge to linked array
				// TODO: assume happens-before relationship is guaranteed 
				// by main thread waiting for all threads finish
				edgeList.add(vertexId, edgeValue);
				nNewEdges++;
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
