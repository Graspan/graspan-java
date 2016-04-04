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

	private static final Logger logger = GraspanLogger
			.getLogger("EdgeComputer");

	private static Vertex[] vertices = null;
	private static ComputationSet[] compSets;
	private static List<LoadedVertexInterval> intervals = null;

	String s = "";

	private Vertex vertex = null;
	private ComputationSet compSet;
	private int nNewEdges, nDupEdges;
	private boolean terminateStatus;

	// vertex id - oldtgt/newtgt - position in row
	private int[][] minPtrs = null;

	public EdgeComputerM(Vertex vertex, ComputationSet compSet) {
		this.vertex = vertex;
		this.compSet = compSet;
		nNewEdges = 0;

		// initialize mergeProgressMarkers
		this.minPtrs = new int[vertices.length][2];
		for (int i = 0; i < vertices.length; i++) {
			this.minPtrs[i][0] = -1;
			this.minPtrs[i][1] = -1;
		}
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

	public static void setVertices(Vertex[] v) {
		vertices = v;
	}

	public static void setIntervals(List<LoadedVertexInterval> vertexIntervals) {
		intervals = vertexIntervals;
	}

	public void execUpdate() {

		// 1. get the compSet components

		// 1.1. we shall scan these to get the targets
		int[] oldEdgs = this.compSet.getOldEdgs();
		int[] newEdgs = this.compSet.getNewEdgs();

		// 1.2. if there is nothing to merge, return
		boolean oldEdgs_empty = false, newEdgs_empty = false;
		if (oldEdgs != null) {
			if (oldEdgs.length == 0) {
				oldEdgs_empty = true;
			} else if (oldEdgs[0] == -1) {
				oldEdgs_empty = true;
			}
		}
		if (newEdgs != null) {
			if (newEdgs.length == 0) {
				newEdgs_empty = true;
			} else if (newEdgs[0] == -1) {
				newEdgs_empty = true;
			}
		}
		if (oldEdgs_empty && newEdgs_empty)
			return;

		// 2. get the rows to merge

		LoadedVertexInterval interval;
		HashSet<Integer> newIdsToMerge = new HashSet<Integer>();
		HashSet<Integer> oldUnewIdsToMerge = new HashSet<Integer>();

		// 2.1. get the ids of the new_components to merge
		int targetRowId = -1;
		int newTgt = -1;
		if (!oldEdgs_empty) {
			for (int i = 0; i < oldEdgs.length; i++) {

				if (oldEdgs[i] == -1)
					break;

				// if the target is not a source vertex
				if (oldEdgs[i] != this.vertex.getVertexId()) {
					newTgt = oldEdgs[i];
					for (int j = 0; j < intervals.size(); j++) {
						interval = intervals.get(j);
						if (newTgt >= interval.getFirstVertex()
								&& newTgt <= interval.getLastVertex()) {
							targetRowId = newTgt - interval.getFirstVertex()
									+ interval.getIndexStart();
							assert (targetRowId != -1);
						}
					}
					if (targetRowId == -1)
						continue;

					if (vertices[targetRowId].getOutEdges().length > 0) {
						// ignore rows that have no outgoing edges

						oldUnewIdsToMerge.add(targetRowId);

					}
				}

			}
		}

		// 2.2. get the ids of the oldUnew_components to merge by scanning new
		// edges
		targetRowId = -1;
		newTgt = -1;
		if (!newEdgs_empty) {
			for (int i = 0; i < newEdgs.length; i++) {
				if (newEdgs[i] == -1)
					break;

				// if the target is not a source vertex
				if (newEdgs[i] != this.vertex.getVertexId()) {
					newTgt = newEdgs[i];
					for (int j = 0; j < intervals.size(); j++) {
						interval = intervals.get(j);
						if (newTgt >= interval.getFirstVertex()
								&& newTgt <= interval.getLastVertex()) {
							// the target lies on this interval

							targetRowId = newTgt - interval.getFirstVertex()
									+ interval.getIndexStart();
							assert (targetRowId != -1);
						}
					}

					if (targetRowId == -1)
						continue;

					if (vertices[targetRowId].getOutEdges().length > 0) {
						// ignore rows that have no outgoing edges

						newIdsToMerge.add(targetRowId);

					}
				}

			}
		}

		// 2.3. if we have found no rows to merge
		if (oldUnewIdsToMerge.size() + newIdsToMerge.size() == 0)
			return;

		int num_of_rows_to_merge = 1 + oldUnewIdsToMerge.size()
				+ newIdsToMerge.size();

		// 3. store the refs to rows in edgArrstoMerge & valArrstoMerge
		int[][] edgArrstoMerge = new int[num_of_rows_to_merge][];
		byte[][] valArrstoMerge = new byte[num_of_rows_to_merge][];

		// 3.1. first store the source row
		int rows_to_merge_id = 0;
		// logger.info("The Id of source vertex: " + this.vertex.getVertexId());
		edgArrstoMerge[0] = this.compSet.getOldUnewEdgs();
		valArrstoMerge[0] = this.compSet.getOldUnewVals();
		int srcRowId = 0;
		rows_to_merge_id++;
//		logger.info("Vertex Id: " + this.vertex.getVertexId()
//				+ " Edge Arrays to merge (source row):"
//				+ Arrays.toString(edgArrstoMerge[0]));

		// 3.2. now store the new component rows
		for (Integer id : newIdsToMerge) {
			edgArrstoMerge[rows_to_merge_id] = compSets[id].getNewEdgs();
			valArrstoMerge[rows_to_merge_id] = compSets[id].getNewVals();
//			logger.info("Vertex Id: " + vertices[id].getVertexId()
//					+ " Edge Arrays to merge (new component rows):"
//					+ Arrays.toString(edgArrstoMerge[rows_to_merge_id]));
			// logger.info("Look This! vertex id#" + vertices[id].getVertexId()
			// + " " + Arrays.toString(vertices[id].getOutEdges()));
			rows_to_merge_id++;
		}

		// 3.3. now store the oldUnew component rows of targets
		for (Integer id : oldUnewIdsToMerge) {
			edgArrstoMerge[rows_to_merge_id] = compSets[id].getOldUnewEdgs();
			valArrstoMerge[rows_to_merge_id] = compSets[id].getOldUnewVals();
//			logger.info("Vertex Id: " + vertices[id].getVertexId()
//					+ " Edge Arrays to merge (oldUnew component rows):"
//					+ Arrays.toString(edgArrstoMerge[rows_to_merge_id]));
			rows_to_merge_id++;
		}

		// logger.info("EdgeArrstoMerge: \n" +
		// Arrays.deepToString(edgArrstoMerge));

		oldUnewIdsToMerge.clear();
		newIdsToMerge.clear();

		// 4. call the SortedArrMerger merge function
		SortedArrMerger sortedArrMerger = new SortedArrMerger();

		logger.info("Vertex ID: " + this.vertex.getVertexId());
		sortedArrMerger
				.mergeTgtstoSrc(edgArrstoMerge, valArrstoMerge, srcRowId);

		this.compSet.setDeltaEdges(sortedArrMerger.get_src_delta_edgs());
		// logger.info(" WILL THIS WORK "
		// + Arrays.toString(this.compSet.getDeltaEdgs()) + " for vertex "
		// + this.vertex.getVertexId());
		this.compSet.setDeltaVals(sortedArrMerger.get_src_delta_vals());
		this.compSet.setOldUnewUdeltaEdgs(sortedArrMerger
				.get_src_oldUnewUdelta_edgs());
		this.compSet.setOldUnewUdeltaVals(sortedArrMerger
				.get_src_oldUnewUdelta_vals());

		// logger.info("deltaEdgs: "
		// + Arrays.toString(this.compSet.getDeltaEdgs())
		// + ", for vertex #" + this.vertex.getVertexId() + " ThreadNo:"
		// + Thread.currentThread().getId());

		nNewEdges = sortedArrMerger.get_num_new_edges();
		// TODO: DOUBLE CHECK NEWEDGES
		// logger.info("NEW EDGES!! " + nNewEdges + " for vertex no."
		// + vertex.getVertexId());

	}
}