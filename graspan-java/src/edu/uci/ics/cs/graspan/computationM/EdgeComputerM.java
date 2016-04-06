package edu.uci.ics.cs.graspan.computationM;

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

	// static fields
//	private static Vertex[] vertices;

	private static ComputationSet[] compSets;

	private static List<LoadedVertexInterval> intervals;

	// object field
	private Vertex vertex;

	private ComputationSet compSet;

	private int nNewEdges, nDupEdges;

	private boolean terminateStatus;

	public EdgeComputerM(Vertex vertex, ComputationSet compSet) {
		this.vertex = vertex;
		this.compSet = compSet;
		nNewEdges = 0;
	}

	public static void setComputationSets(ComputationSet[] csets) {
		compSets = csets;
	}

	public ComputationSet[] getComputationSets() {
		return compSets;
	}

	public ComputationSet getSrcComputationSet() {
		return compSet;
	}

	public int getNumNewEdges() {
		return nNewEdges;
	}

	public void setNumNewEdges(int nNewEdges) {
		this.nNewEdges = nNewEdges;
	}

	public int getNumDupEdges() {
		return nDupEdges;
	}

	public void setNumDupEdges(int nDupEdges) {
		this.nDupEdges = nDupEdges;
	}

	public boolean getTerminateStatus() {
		return terminateStatus;
	}

	public void setTerminateStatus(boolean terminateStatus) {
		this.terminateStatus = terminateStatus;
	}

//	public static void setVertices(Vertex[] v) {
//		vertices = v;
//	}

	public static void setIntervals(List<LoadedVertexInterval> vertexIntervals) {
		intervals = vertexIntervals;
	}

	public void execUpdate() {

		// 1. get the compSet components
		// 1.1. we shall scan these to get the targets
		int[] oldEdgs = this.compSet.getOldEdgs();
		int[] newEdgs = this.compSet.getNewEdgs();

		// 1.2. if there is nothing to merge, return
		boolean oldEdgs_empty = true, newEdgs_empty = true;
		if (oldEdgs != null && oldEdgs.length != 0 && oldEdgs[0] != -1) {
			oldEdgs_empty = false;
		}
		if (newEdgs != null && newEdgs.length != 0 && newEdgs[0] != -1) {
			newEdgs_empty = false;
		}
		if (oldEdgs_empty && newEdgs_empty)
			return;

		int[][] edgArrstoMerge = null;
		byte[][] valArrstoMerge = null;

		// 2. get the rows to merge
		HashSet<Integer> newIdsToMerge = new HashSet<Integer>();
		HashSet<Integer> oldUnewIdsToMerge = new HashSet<Integer>();

		// 2.1. get the ids of the new_components to merge
		getRowIdsToMerge(oldEdgs, oldEdgs_empty, newIdsToMerge, "old");

		// 2.2. get the ids of the oldUnew_components to merge by scanning new edges
		getRowIdsToMerge(newEdgs, newEdgs_empty, oldUnewIdsToMerge, "new");

		int num_of_rows_to_merge = 1 + oldUnewIdsToMerge.size() + newIdsToMerge.size();

		// 3. store the refs to rows in edgArrstoMerge & valArrstoMerge
		edgArrstoMerge = new int[num_of_rows_to_merge][];
		valArrstoMerge = new byte[num_of_rows_to_merge][];

		// 3.1. first store the source row
		int rows_to_merge_id = 0;
		// logger.info("The Id of source vertex: " + this.vertex.getVertexId());
		edgArrstoMerge[0] = this.compSet.getOldUnewEdgs();
		valArrstoMerge[0] = this.compSet.getOldUnewVals();

		rows_to_merge_id++;
		// logger.info("Vertex Id: " + this.vertex.getVertexId() + " Edge Arrays to merge (source row):" + Arrays.toString(edgArrstoMerge[0]));

		// 3.2. now store the new component rows
		for (Integer id : newIdsToMerge) {
			edgArrstoMerge[rows_to_merge_id] = compSets[id].getNewEdgs();
			valArrstoMerge[rows_to_merge_id] = compSets[id].getNewVals();
			rows_to_merge_id++;
		}

		// 3.3. now store the oldUnew component rows of targets
		for (Integer id : oldUnewIdsToMerge) {
			edgArrstoMerge[rows_to_merge_id] = compSets[id].getOldUnewEdgs();
			valArrstoMerge[rows_to_merge_id] = compSets[id].getOldUnewVals();
			rows_to_merge_id++;
		}

		// logger.info("EdgeArrstoMerge: \n" +
		// Arrays.deepToString(edgArrstoMerge));

		oldUnewIdsToMerge.clear();
		newIdsToMerge.clear();

		// -------------------------------------------------------------------------------
		// 4. call the SortedArrMerger merge function
		SortedArrMerger sortedArrMerger = new SortedArrMerger();

		// logger.info("Vertex ID: " + this.vertex.getVertexId());
		int srcRowId = 0;
		sortedArrMerger.mergeTgtstoSrc(edgArrstoMerge, valArrstoMerge, srcRowId);

		// -------------------------------------------------------------------------------
		this.compSet.setDeltaEdges(sortedArrMerger.get_src_delta_edgs());
		this.compSet.setDeltaVals(sortedArrMerger.get_src_delta_vals());
		this.compSet.setOldUnewUdeltaEdgs(sortedArrMerger.get_src_oldUnewUdelta_edgs());
		this.compSet.setOldUnewUdeltaVals(sortedArrMerger.get_src_oldUnewUdelta_vals());

		// get number of new edges
		nNewEdges = sortedArrMerger.get_num_new_edges();

	}

	private void getRowIdsToMerge(int[] edgs, boolean edgs_empty, HashSet<Integer> idsToMerge, String flag) {
		int targetRowId = -1;
		LoadedVertexInterval interval;
		int newTgt = -1;
		if (!edgs_empty) {
			for (int i = 0; i < edgs.length; i++) {
				if (edgs[i] == -1)
					break;

				// if the target is not a source vertex
				if (edgs[i] != this.vertex.getVertexId()) {
					newTgt = edgs[i];
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
						idsToMerge.add(targetRowId);
					}
				}

			}
		}
	}
	
	
	
}