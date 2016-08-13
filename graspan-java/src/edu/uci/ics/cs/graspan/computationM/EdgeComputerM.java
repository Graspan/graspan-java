package edu.uci.ics.cs.graspan.computationM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.ComputationSet;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * @author Aftab Hussain
 * 
 *         Created by Mar 18, 2016
 */
public class EdgeComputerM {

	private static final Logger logger = GraspanLogger.getLogger("EdgeComputer");

	/**
	 * 
	 * @param i
	 * @param compSets
	 * @param intervals
	 * @return
	 */
	public static long execUpdate(int i, ComputationSet[] compSets, List<LoadedVertexInterval> intervals) {
		
		
		ComputationSet compSet = compSets[i];
		
		// 1. get the compSet components
		// 1.1. we shall scan these to get the targets
		int[] oldEdgs = compSet.getOldEdgs();
		byte[] oldVals = compSet.getOldVals();
		
		int[] newEdgs = compSet.getNewEdgs();
		byte[] newVals = compSet.getNewVals();

		boolean oldEdgs_empty = true, newEdgs_empty = true;
		if (oldEdgs.length != 0) {
			oldEdgs_empty = false;
		}
		if (newEdgs.length != 0) {
			newEdgs_empty = false;
		}
		
		// 1.2. if there is nothing to merge, return
		if (oldEdgs_empty && newEdgs_empty)
			return 0;
		
//		if (vertices[i].getVertexId() == 4017) {
//			logger.info("oldEdgs" + Arrays.toString(oldEdgs));
//			logger.info("oldVals" + Arrays.toString(oldVals));
//			logger.info("newEdgs" + Arrays.toString(newEdgs));
//			logger.info("newVals" + Arrays.toString(newVals));
//		}

		// 2. get the rows to merge
		HashSet<IdValuePair> newRowIndicesToMerge = new HashSet<IdValuePair>();
		HashSet<IdValuePair> oldUnewRowIndicesToMerge = new HashSet<IdValuePair>();

		// 2.1. get the indices of the new_components to merge
		getRowIndicesToMerge(compSets, intervals, oldEdgs, oldVals, oldEdgs_empty, newRowIndicesToMerge, "old");
		
		// 2.2. get the indices of the oldUnew_components to merge 
		getRowIndicesToMerge(compSets, intervals, newEdgs, newVals, newEdgs_empty, oldUnewRowIndicesToMerge, "new");
		
//		if (vertices[i].getVertexId() == 4017) {
//			for (IdValuePair ivp : newRowIndicesToMerge) {
//				logger.info("newRowIndicesToMerge - " + ivp.id + " " + ivp.value);//+ " The vertex"+vertices[ivp.id]);
//			}
//			for (IdValuePair ivp : oldUnewRowIndicesToMerge) {
//				logger.info("oldUnewRowIndicesToMerge - " + ivp.id + " " + ivp.value);//+" The vertex"+vertices[ivp.id]);
//			}
//		}

		
		int num_of_rows_to_merge = 2 + oldUnewRowIndicesToMerge.size() + newRowIndicesToMerge.size();
		// 3. store the refs to rows in edgArrstoMerge & valArrstoMerge
		int[][] edgArrstoMerge = new int[num_of_rows_to_merge][];
		byte[][] valArrstoMerge = new byte[num_of_rows_to_merge][];
		// 3.1. first store the source row
		int rows_to_merge_id = 0;
		edgArrstoMerge[0] = compSet.getOldUnewEdgs();
		valArrstoMerge[0] = compSet.getOldUnewVals();
		rows_to_merge_id++;
		
		//for singleton rule
		rows_to_merge_id = genEdgesToMergeForSRule(newEdgs, newVals, edgArrstoMerge, valArrstoMerge, rows_to_merge_id); //TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT.
		
		//for length 2 rule
		rows_to_merge_id = genEdgesToMerge(compSets, newRowIndicesToMerge, edgArrstoMerge, valArrstoMerge, rows_to_merge_id, "old");
		rows_to_merge_id = genEdgesToMerge(compSets, oldUnewRowIndicesToMerge, edgArrstoMerge, valArrstoMerge, rows_to_merge_id, "new");
		

		//-------------------------------------------------------------------------------
		// 4. call the SortedArrMerger merge function
		SortedArrMerger sortedArrMerger = new SortedArrMerger();
		// logger.info("Vertex ID: " + this.vertex.getVertexId());
		int srcRowId = 0;
		sortedArrMerger.mergeTgtstoSrc(edgArrstoMerge, valArrstoMerge, srcRowId); //TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT.

		//-------------------------------------------------------------------------------
		compSet.setDeltaEdges(sortedArrMerger.get_src_delta_edgs());
		compSet.setDeltaVals(sortedArrMerger.get_src_delta_vals());
		compSet.setOldUnewUdeltaEdgs(sortedArrMerger.get_src_oldUnewUdelta_edgs());
		compSet.setOldUnewUdeltaVals(sortedArrMerger.get_src_oldUnewUdelta_vals());

		// get number of new edges
		return sortedArrMerger.get_num_new_edges();

	}


	/**
	 * 
	 * @param newEdgs
	 * @param newVals
	 * @param edgArrstoMerge
	 * @param valArrstoMerge
	 * @param rows_to_merge_id
	 * @return
	 */
	private static int genEdgesToMergeForSRule(int[] newEdgs, byte[] newVals, int[][] edgArrstoMerge, byte[][] valArrstoMerge, int rows_to_merge_id) {
		//get the resulting edge and value array
		List<IdValuePair> list = new ArrayList<IdValuePair>();//TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. 
		for(int i = 0; i < newEdgs.length; i++){
			int dstId = newEdgs[i];
			byte dstVal = newVals[i];
			
			byte newVal = GrammarChecker.checkL1Rules(dstVal);
			if(newVal != -1){
				list.add(new IdValuePair(dstId, newVal));
			}
		}
		
//		if(!list.isEmpty()){
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
//		}
		
		return rows_to_merge_id;
	}


	/**
	 * 
	 * @param compSets
	 * @param idsToMerge
	 * @param edgArrstoMerge
	 * @param valArrstoMerge
	 * @param rows_to_merge_id
	 * @param flag
	 * @return
	 */
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
				
				byte newVal = GrammarChecker.checkL2Rules(srcVal, dstVal);
				if(newVal != -1){
					list.add(new IdValuePair(dstId, newVal));
				}
			}
			
//			if(!list.isEmpty()){
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
//			}
		}
		
		return rows_to_merge_id;
	}
	
//	private static byte checkGrammarAndGetNewEdgeVal(byte edgeVal1, byte edgeVal2) {
//
//		byte[][] grammarTab = GlobalParams.getGrammarTab();
//		byte edgeVal3 = -1;
//		for (int i = 0; i < grammarTab.length; i++) {
//			if (grammarTab[i][0] == edgeVal1 && grammarTab[i][1] == edgeVal2) {
//				edgeVal3 = grammarTab[i][2];
//				break;
//			}
//		}
//		return edgeVal3;
//	}

	/**
	 * 
	 * @param compSets
	 * @param intervals
	 * @param edgs
	 * @param vals
	 * @param edgs_empty
	 * @param newIdsToMerge
	 * @param flag
	 */
	private static void getRowIndicesToMerge(ComputationSet[] compSets, List<LoadedVertexInterval> intervals, int[] edgs, byte[] vals, boolean edgs_empty, HashSet<IdValuePair> newIdsToMerge, String flag) {
		int targetRowIndex = -1;
		LoadedVertexInterval interval;
		
		int newTgt = 0;
		byte val = 0;
		
		if (!edgs_empty) {
			for (int i = 0; i < edgs.length; i++) {
				newTgt = edgs[i];
				val = vals[i];
				
				for (int j = 0; j < intervals.size(); j++) {
					targetRowIndex = -1;
					interval = intervals.get(j);
					if (newTgt >= interval.getFirstVertex() && newTgt <= interval.getLastVertex()) { // chk if newTgt is among loaded src vertices
						targetRowIndex = newTgt - interval.getFirstVertex() + interval.getIndexStart();
						assert (targetRowIndex != -1);
						break;
					}
				}
				if (targetRowIndex == -1)
					continue;
				
				if((flag.equals("old") && compSets[targetRowIndex].getNewEdgs().length > 0) 
						|| (flag.equals("new") && compSets[targetRowIndex].getOldUnewEdgs().length > 0)){
					newIdsToMerge.add(new IdValuePair(targetRowIndex, val));
				}
//				if (edgs[i] != this.vertex.getVertexId()) {
//				}

			}
		}
	}

	static class IdValuePair {
		private final int id;
		private final byte value;

		IdValuePair(int id, byte value) {
			this.id = id;
			this.value = value;
		}

		public int hashCode() {
			int r = 1;
			r = r * 31 + this.id;
			r = r * 31 + this.value;
			return r;
		}

		public boolean equals(Object o) {
			return (o instanceof IdValuePair) && (((IdValuePair) o).id == id) && (((IdValuePair) o).value == value);
		}
	}
	
}