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

	// set of new intervals (the last source vertex Ids of each partition)
	private static TreeSet<Integer> newPartLimits;

	// set of partitions that have been repartitioned
	private static HashSet<Integer> repartitionedParts;

	// set of partitions that have been newly generated from
	// repartitioning
	private static HashSet<Integer> newPartsFrmRepartitioning;

	// set of partitions that have new edges but have not been repartitioned
	private static HashSet<Integer> modifiedParts;

	// set of partitions to which no new edges were added
	private static HashSet<Integer> unModifiedParts;

	// set of all partitions in the memory after repartitioning
	private static HashSet<Integer> loadedPartsPostProcessing;

	// set of partitions that are to be saved (depends on partition reload
	// strategy)
	private static HashSet<Integer> partsToSave;

	public static void initRepartioningVars() {
		splitVertices = new ArrayList<Integer>();
		newPartLimits = new TreeSet<Integer>();
		repartitionedParts = new HashSet<Integer>();
		newPartsFrmRepartitioning = new HashSet<Integer>();
		modifiedParts = new HashSet<Integer>();
		unModifiedParts = new HashSet<Integer>();
		loadedPartsPostProcessing = new HashSet<Integer>();
		partsToSave = new HashSet<Integer>();
	}

	public static HashSet<Integer> getModifiedParts() {
		return modifiedParts;
	}

	public static HashSet<Integer> getUnModifiedParts() {
		return unModifiedParts;
	}

	public static ArrayList<Integer> getSplitVertices() {
		return splitVertices;
	}

	public static TreeSet<Integer> getNewPartLimits() {
		return newPartLimits;
	}

	public static HashSet<Integer> getRepartitionedParts() {
		return repartitionedParts;
	}

	public static HashSet<Integer> getNewPartsFrmRepartitioning() {
		return newPartsFrmRepartitioning;
	}

	public static void clearRepartitioningVars() {
		splitVertices.clear();
		newPartLimits.clear();
		repartitionedParts.clear();
		newPartsFrmRepartitioning.clear();
		modifiedParts.clear();
		unModifiedParts.clear();
		loadedPartsPostProcessing.clear();
		partsToSave.clear();
	}

	public static HashSet<Integer> getPartsToSave() {
		return partsToSave;
	}

	public static HashSet<Integer> getLoadedPartsPostProcessing() {
		return loadedPartsPostProcessing;
	}

}
