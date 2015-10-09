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
			GraphDTCVertex[] verticesTo, GraphDTCNewEdgesList edgeList) {
		if(vertex == null || verticesFrom == null || 
				verticesTo == null || edgeList == null)
			return;
		
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
