package edu.uci.ics.cs.gdtc.edgecomputation;

import edu.uci.ics.gdtc.GraphDTCNewEdgesList;
import edu.uci.ics.gdtc.GraphDTCVertex;

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
			char edgeValue = vertex.getOutEdgeValue(i);
			
		}
		scanAddableEdges();
		checkDuplication();
		addEdges();
	}

	/**
	 * Description:
	 * @param:
	 * @return:void
	 */
	private static void addEdges() {
		
	}

	/**
	 * Description:
	 * @param:
	 * @return:void
	 */
	private static void checkDuplication() {
		
	}

	/**
	 * Description:
	 * @param:
	 * @return:void
	 */
	private static void scanAddableEdges() {
		
	}
}
