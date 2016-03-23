package edu.uci.ics.cs.graspan.computationM;

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
	private static ComputationSet[] compSets = null;
	private static List<LoadedVertexInterval> intervals = null;

	String s = "";

	private Vertex vertex = null;
	private ComputationSet compSet = null;
	private int nNewEdges, nDupEdges;
	private boolean terminateStatus;

	// vertex id - oldtgt/newtgt - position in row
	private int[][] minPtrs = null;

	public EdgeComputerM(Vertex vertex, ComputationSet computationSet) {
		this.vertex = vertex;
		this.compSet = computationSet;

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

	public ComputationSet[] getComputationSet() {
		return compSets;
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

		// initialize the computationSet for the base row

		int[] oldTgts_BaseRow = this.compSet.getOldTgts();
		byte[] oldTgtsEdgeVals_BaseRow = this.compSet.getOldTgtEdgeVals();
		int[] newTgts_BaseRow = this.compSet.getNewTgts();
		byte[] newTgtEdgeVals_BaseRow = this.compSet.getNewTgtEdgeVals();
		int[] op_BaseRow = new int[newTgts_BaseRow.length];
		byte[] opEdgeVals_BaseRow = new byte[newTgts_BaseRow.length];
		this.compSet.setOp(op_BaseRow);
		this.compSet.setOpEdgeVals(opEdgeVals_BaseRow);
		compSets[this.vertex.getVertexIdx()].setOp(op_BaseRow);
		compSets[this.vertex.getVertexIdx()].setOpEdgeVals(opEdgeVals_BaseRow);

		// get the ids of the target rows to which the above base row points to

		int targetRowId = -1;
		LoadedVertexInterval interval;
		HashSet<Integer> targetRowIds = new HashSet<Integer>();

		// TODO: to consider case when newTgts_BaseRow is null
		int newTgt;
		for (int i = 0; i < newTgts_BaseRow.length; i++) {
			newTgt = newTgts_BaseRow[i];
			for (int j = 0; j < intervals.size(); j++) {
				interval = intervals.get(j);
				if (newTgt >= interval.getFirstVertex()
						& newTgt <= interval.getLastVertex()) {
					targetRowId = newTgt - interval.getFirstVertex()
							+ interval.getIndexStart();
					assert (targetRowId != -1);
				}
			}
			if (targetRowId == -1)
				continue;
			targetRowIds.add(targetRowId);
		}

		// logger.info("SEE THIS " + targetRowIds + " ");

		// set up merge process pointers for these targetRowIds

		int[] oldTgts_TargetRow;
		int[] newTgts_TargetRow;

		for (int id : targetRowIds) {

			oldTgts_TargetRow = compSets[id].getOldTgts();
			newTgts_TargetRow = compSets[id].getNewTgts();

			if (oldTgts_TargetRow != null) {
				if (oldTgts_TargetRow.length != 0) {
					// position at target row with id "id" is 0
					this.minPtrs[id][0] = 0;

					// indicate that this corresponds to the oldTgtSet
					this.minPtrs[id][1] = 0;
				}

			} else if (oldTgts_TargetRow == null & newTgts_TargetRow != null) {
				if (newTgts_TargetRow.length != 0) {
					// position at target row with id "id" is 0
					this.minPtrs[id][0] = 0;

					// indicate that this corresponds to the newTgtSet
					this.minPtrs[id][1] = 1;
				}
			}
		}

		// get the minimum target vertex
		int minTgt = Integer.MAX_VALUE;
		int minTgtRowId = -1;
		HashSet<Byte> minTgtEdgeVals = new HashSet<Byte>();

		for (int id : targetRowIds) {

			if (this.minPtrs[id][1] == 0) {
				// marker is in oldTgts for this row
				if (compSets[id].getOldTgts()[this.minPtrs[id][0]] <= minTgt) {
					minTgt = compSets[id].getOldTgts()[this.minPtrs[id][0]];
					if (!minTgtEdgeVals.isEmpty())
						minTgtEdgeVals.clear();
					minTgtEdgeVals
							.add(compSets[id].getOldTgtEdgeVals()[this.minPtrs[id][0]]);
					minTgtRowId = id;
				}

			} else if (this.minPtrs[id][1] == 1) {
				// marker is in newTgts for this row
				if (compSets[id].getNewTgts()[this.minPtrs[id][0]] <= minTgt) {
					minTgt = compSets[id].getNewTgts()[this.minPtrs[id][0]];
					if (!minTgtEdgeVals.isEmpty())
						minTgtEdgeVals.clear();
					minTgtEdgeVals
							.add(compSets[id].getNewTgtEdgeVals()[this.minPtrs[id][0]]);
					minTgtRowId = id;
				}
			}

		}

	}
}