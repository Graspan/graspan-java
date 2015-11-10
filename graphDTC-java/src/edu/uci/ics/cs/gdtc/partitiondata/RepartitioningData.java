package edu.uci.ics.cs.gdtc.partitiondata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

/**
 * 
 * @author Aftab 9 Nov 2015
 *
 */
public class RepartitioningData {
	static // splitVertices
	ArrayList<Integer> splitVertices = new ArrayList<Integer>();

	static // set of new intervals
	TreeSet<Integer> newIntervals = new TreeSet<Integer>();

	// set of partitions that have been repartitioned
	static HashSet<Integer> repartitionedParts = new HashSet<Integer>();

	static // set of partitions that have been newly generated from
			// repartitioning
	HashSet<Integer> newPartsFrmRepartitioning = new HashSet<Integer>();

	static // set of partitions that have been newly generated from
			// repartitioning
	HashSet<Integer> modifiedParts = new HashSet<Integer>();

	static // set of partitions that have been newly generated from
			// repartitioning
	HashSet<Integer> unchangedParts = new HashSet<Integer>();

	public void initRepartioningVars() {
		splitVertices = new ArrayList<Integer>();
		newIntervals = new TreeSet<Integer>();
		repartitionedParts = new HashSet<Integer>();
		newPartsFrmRepartitioning = new HashSet<Integer>();
		modifiedParts = new HashSet<Integer>();
		unchangedParts = new HashSet<Integer>();
	}

	public static HashSet<Integer> getModifiedParts() {
		return modifiedParts;
	}

	public static HashSet<Integer> getUnchangedParts() {
		return unchangedParts;
	}

	public static ArrayList<Integer> getSplitVertices() {
		return splitVertices;
	}

	public static TreeSet<Integer> getNewIntervals() {
		return newIntervals;
	}

	public static HashSet<Integer> getRepartitionedParts() {
		return repartitionedParts;
	}

	public static HashSet<Integer> getNewPartsFrmRepartitioning() {
		return newPartsFrmRepartitioning;
	}

	public static void clearRepartitioningVars() {
		splitVertices.clear();
		newIntervals.clear();
		repartitionedParts.clear();
		newPartsFrmRepartitioning.clear();
		modifiedParts.clear();
		unchangedParts.clear();
	}

}
