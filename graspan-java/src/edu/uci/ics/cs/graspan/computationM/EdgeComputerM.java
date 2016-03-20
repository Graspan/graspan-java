package edu.uci.ics.cs.graspan.computationM;

import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.NewEdgesList;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * @author Aftab Hussain
 * 
 *         Created by Mar 18, 2016
 */
public class EdgeComputerM {

	private static final Logger logger = GraspanLogger
			.getLogger("EdgeComputer");

	// test flag for viewing new edges of a source
	public static int flag = 0;
	String s = "";

	private Vertex vertex = null;
	private int nNewEdges, nDupEdges;
	private boolean terminateStatus;
	private static Vertex[] vertices = null;
	private static List<LoadedVertexInterval> intervals = null;

	// computation set
	private int oldTargets[] = null;
	private int newTargets[] = null;
	private int computationOP[] = null;

	public int[] getOldTargets() {
		return oldTargets;
	}

	public void setOldTargets(int[] oldTargets) {
		this.oldTargets = oldTargets;
	}

	public int[] getNewTargets() {
		return newTargets;
	}

	public void setNewTargets(int[] newTargets) {
		this.newTargets = newTargets;
	}

	public int[] getCompOP() {
		return computationOP;
	}

	public void setCompOP(int[] computationOP) {
		this.computationOP = computationOP;
	}

	public EdgeComputerM(Vertex vertex) {
		this.vertex = vertex;
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
	}

}