package edu.uci.ics.cs.graspan.computationM;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.ComputationSet;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * @author Aftab Hussain
 * 
 *         Created by Mar 18, 2016
 */
public class EdgeComputerM {

	private static final Logger logger = GraspanLogger.getLogger("EdgeComputer");


	public static long execUpdate(int i, ComputationSet[] compSets, List<LoadedVertexInterval> intervals) {
		ComputationSet compSet = compSets[i];
		
		// 1. get the compSet components
		// 1.1. we shall scan these to get the targets
		int[] oldEdgs = compSet.getOldEdgs();
		byte[] oldVals = compSet.getOldVals();
		
		int[] newEdgs = compSet.getNewEdgs();
		byte[] newVals = compSet.getNewVals();

		// 1.2. if there is nothing to merge, return
		boolean oldEdgs_empty = true, newEdgs_empty = true;
		if (oldEdgs != null && oldEdgs.length != 0 && oldEdgs[0] != -1) {
			oldEdgs_empty = false;
		}
		if (newEdgs != null && newEdgs.length != 0 && newEdgs[0] != -1) {
			newEdgs_empty = false;
		}
		if (oldEdgs_empty && newEdgs_empty)
			return 0;

		int[][] edgArrstoMerge = null;
		byte[][] valArrstoMerge = null;

		// 2. get the rows to merge
		HashSet<IdValuePair> newIdsToMerge = new HashSet<IdValuePair>();
		HashSet<IdValuePair> oldUnewIdsToMerge = new HashSet<IdValuePair>();

		// 2.1. get the ids of the new_components to merge
		getRowIdsToMerge(compSets, intervals, oldEdgs, oldVals, oldEdgs_empty, newIdsToMerge, "old");

		// 2.2. get the ids of the oldUnew_components to merge by scanning new edges
		getRowIdsToMerge(compSets, intervals, newEdgs, newVals, newEdgs_empty, oldUnewIdsToMerge, "new");

		int num_of_rows_to_merge = 1 + oldUnewIdsToMerge.size() + newIdsToMerge.size();

		// 3. store the refs to rows in edgArrstoMerge & valArrstoMerge
		edgArrstoMerge = new int[num_of_rows_to_merge][];
		valArrstoMerge = new byte[num_of_rows_to_merge][];

		// 3.1. first store the source row
		int rows_to_merge_id = 0;
		// logger.info("The Id of source vertex: " + this.vertex.getVertexId());
		edgArrstoMerge[0] = compSet.getOldUnewEdgs();
		valArrstoMerge[0] = compSet.getOldUnewVals();

		rows_to_merge_id++;
		// logger.info("Vertex Id: " + this.vertex.getVertexId() + " Edge Arrays to merge (source row):" + Arrays.toString(edgArrstoMerge[0]));

		rows_to_merge_id = genEdgesToMerge(compSets, newIdsToMerge, edgArrstoMerge, valArrstoMerge, rows_to_merge_id, "old");
		rows_to_merge_id = genEdgesToMerge(compSets, oldUnewIdsToMerge, edgArrstoMerge, valArrstoMerge, rows_to_merge_id, "new");
		
//		// 3.2. now store the new component rows
//		for (Integer id : newIdsToMerge) {
//			edgArrstoMerge[rows_to_merge_id] = compSets[id].getNewEdgs();
//			valArrstoMerge[rows_to_merge_id] = compSets[id].getNewVals();
//			rows_to_merge_id++;
//		}
//
//		// 3.3. now store the oldUnew component rows of targets
//		for (Integer id : oldUnewIdsToMerge) {
//			edgArrstoMerge[rows_to_merge_id] = compSets[id].getOldUnewEdgs();
//			valArrstoMerge[rows_to_merge_id] = compSets[id].getOldUnewVals();
//			rows_to_merge_id++;
//		}

		// logger.info("EdgeArrstoMerge: \n" +
		// Arrays.deepToString(edgArrstoMerge));

		

		// -------------------------------------------------------------------------------
		// 4. call the SortedArrMerger merge function
		SortedArrMerger sortedArrMerger = new SortedArrMerger();

		// logger.info("Vertex ID: " + this.vertex.getVertexId());
		int srcRowId = 0;
		sortedArrMerger.mergeTgtstoSrc(edgArrstoMerge, valArrstoMerge, srcRowId);

		// -------------------------------------------------------------------------------
		compSet.setDeltaEdges(sortedArrMerger.get_src_delta_edgs());
		compSet.setDeltaVals(sortedArrMerger.get_src_delta_vals());
		compSet.setOldUnewUdeltaEdgs(sortedArrMerger.get_src_oldUnewUdelta_edgs());
		compSet.setOldUnewUdeltaVals(sortedArrMerger.get_src_oldUnewUdelta_vals());

		// get number of new edges
		return sortedArrMerger.get_num_new_edges();

	}

//	private static void generateEdgesToMerge(int[][] edgArrstoMerge, byte[][] valArrstoMerge,
//			HashSet<IdValuePair> newIdsToMerge, HashSet<IdValuePair> oldUnewIdsToMerge, int rows_to_merge_id, ComputationSet[] compSets) {
//		// TODO Auto-generated method stub
//		genEdgesToMerge(compSets, newIdsToMerge, edgArrstoMerge, valArrstoMerge, rows_to_merge_id);
//		
//		
//		
//	}

	private static int genEdgesToMerge(ComputationSet[] compSets, HashSet<IdValuePair> idsToMerge,
			int[][] edgArrstoMerge, byte[][] valArrstoMerge, int rows_to_merge_id, String flag) {
		
		for(IdValuePair pair: idsToMerge){
			int index = pair.id;
			byte srcVal = pair.value;

			//
			int[] edges = null;
			byte[] vals = null;
			
			if(flag.equals("old")){
				edges = compSets[index].getNewEdgs();
				vals = compSets[index].getNewVals();
			}
			else if(flag.equals("new")){
				edges = compSets[index].getOldUnewEdgs();
				vals = compSets[index].getOldUnewVals();
			}
			
			//get the resulting edge and value array
			List<IdValuePair> list = new ArrayList<IdValuePair>();
			for(int i = 0; i < edges.length; i++){
				int dstId = edges[i];
				byte dstVal = vals[i];
				
				byte newVal = checkGrammarAndGetNewEdgeVal(srcVal, dstVal);
				if(newVal != -1){
					list.add(new IdValuePair(dstId, newVal));
				}
			}
			
			int[] newEdgeArray = new int[list.size()];
			byte[] newValArray = new byte[list.size()];
			for(int i = 0; i < list.size(); i++){
				IdValuePair ele = list.get(i);
				newEdgeArray[i] = ele.id;
				newValArray[i] = ele.value;
			}
				
			edgArrstoMerge[rows_to_merge_id] = newEdgeArray;
			valArrstoMerge[rows_to_merge_id] = newValArray;
			rows_to_merge_id++;
		}
		
		return rows_to_merge_id;
	}
	
	private static byte checkGrammarAndGetNewEdgeVal(byte edgeVal1, byte edgeVal2) {

		byte[][] grammarTab = GlobalParams.getGrammarTab();
		byte edgeVal3 = -1;
		for (int i = 0; i < grammarTab.length; i++) {
			if (grammarTab[i][0] == edgeVal1 && grammarTab[i][1] == edgeVal2) {
				edgeVal3 = grammarTab[i][2];
				break;
			}
		}
		return edgeVal3;
	}

	private static void getRowIdsToMerge(ComputationSet[] compSets, List<LoadedVertexInterval> intervals, int[] edgs, byte[] vals, boolean edgs_empty, HashSet<IdValuePair> newIdsToMerge, String flag) {
		int targetRowId = -1;
		LoadedVertexInterval interval;
		
		int newTgt = 0;
		byte val = 0;
		
		if (!edgs_empty) {
			for (int i = 0; i < edgs.length; i++) {
//				if (edgs[i] == -1)
//					break;

				newTgt = edgs[i];
				val = vals[i];
				
				for (int j = 0; j < intervals.size(); j++) {
					interval = intervals.get(j);
					if (newTgt >= interval.getFirstVertex() && newTgt <= interval.getLastVertex()) {
						targetRowId = newTgt - interval.getFirstVertex() + interval.getIndexStart();
						assert (targetRowId != -1);
					}
				}
				if (targetRowId == -1)
					continue;
				
				if((flag.equals("old") && compSets[targetRowId].getNewEdgs().length > 0) 
						|| (flag.equals("new") && compSets[targetRowId].getOldUnewEdgs().length > 0)){
					newIdsToMerge.add(new IdValuePair(targetRowId, val));
				}
//				if (edgs[i] != this.vertex.getVertexId()) {
//				}

			}
		}
	}
	


	static class IdValuePair{
		private final int id;
		private final byte value;
		
		IdValuePair(int id, byte value){
			this.id = id;
			this.value = value;
		}
		
		public int hashCode(){
			int r = 1;
			r = r * 31 + this.id;
			r = r * 31 + this.value;
			return r;
		}
		
		public boolean equals(Object o){
			return (o instanceof IdValuePair) && (((IdValuePair) o).id == id) && (((IdValuePair) o).value == value);
		}
		
		
		
	}
	
}