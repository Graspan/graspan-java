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
	// splitVertices
	private static ArrayList<Integer> splitVertices;

	// set of new intervals
	private static TreeSet<Integer> newIntervals;

	// set of partitions that have been repartitioned
	private static HashSet<Integer> repartitionedParts;

	// set of partitions that have been newly generated from
	// repartitioning
	private static HashSet<Integer> newPartsFrmRepartitioning;

	// set of partitions that have been newly generated from
	// repartitioning
	private static HashSet<Integer> modifiedParts;

	// set of partitions that have been newly generated from
	// repartitioning
	private static HashSet<Integer> unchangedParts;

	// set of partitions that are to be saved (depends on partition reload
	// strategy)
	private static HashSet<Integer> partsToSave;

	public static void initRepartioningVars() {
		splitVertices = new ArrayList<Integer>();
		newIntervals = new TreeSet<Integer>();
		repartitionedParts = new HashSet<Integer>();
		newPartsFrmRepartitioning = new HashSet<Integer>();
		modifiedParts = new HashSet<Integer>();
		unchangedParts = new HashSet<Integer>();
		partsToSave = new HashSet<Integer>();
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

	public static HashSet<Integer> getPartsToSave() {
		return partsToSave;
	}

}
