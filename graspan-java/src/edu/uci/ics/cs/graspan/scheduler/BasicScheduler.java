package edu.uci.ics.cs.graspan.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.cs.graspan.datastructures.GlobalParams;

/**
 * 
 * CURRENTLY WORKS FOR ONLY TWO PARTITIONS LOADED IN THE MEMORY - Schedules the
 * selection of partitions on which memory computations is to be done, also sets
 * a limit of the maximum allowable number of new partitions that can be
 * generated from repartitioning
 * 
 * @author Aftab
 *
 */
public class BasicScheduler implements IScheduler {

	// // We are assuming at most 50 - numOfOriginalParts will be created after
	// the
	// // complete process
	// private static final int SizeOfPartScheduleMap = 50;

	private int[][] partTerminationMap = new int[GlobalParams.getNumParts()][GlobalParams.getNumParts()];

	/**
	 * Initializes the scheduler. An entry of -1 in {@code partTerminationMap}
	 * shows no active partition is represented by this row/column. An entry of
	 * 0 shows this partition pair has not been computed. An entry of 1 shows
	 * this partition pair has been computed.
	 */
	public void initScheduler() {
		int totalNumParts = GlobalParams.getNumParts();
		// initialize partTerminationMap
		for (int i = 0; i < totalNumParts; i++) {
			for (int j = 0; j < i; j++) {
				partTerminationMap[i][j] = 0;
			}
		}
	}

	public void updateSchedulerInfo() {

	}

	/**
	 * Returns the next set of partitions (ids) to be computed TODO currently
	 * designed to load two partitions only.
	 */
	public int[] getPartstoLoad() {

		int numPartsPerComputation = GlobalParams.getNumPartsPerComputation();
		for (int i = 0; i < partTerminationMap.length; i++) {
			for (int j = 0; j < i; j++) {
				if (!isComputed(i, j)) {
					int[] partsToLoad = new int[numPartsPerComputation];
					partsToLoad[0] = i;
					partsToLoad[1] = j;
					partTerminationMap[i][j] = 1;
					return partsToLoad;
				}
			}
		}
		return null;
	}

	public int[][] getPartScheduleMap() {
		return partTerminationMap;
	}

	/**
	 * Checks whether the partition-pair has been computed. Called by
	 * getPartstoLoad()
	 * 
	 * @param part1
	 * @param part2
	 * @return
	 */
	private boolean isComputed(int part1mapId, int part2mapId) {
		if (partTerminationMap[part1mapId][part2mapId] == 1)
			return true;
		else
			return false;
	}

	public void generateComputationPairs() {
		List<Integer> lst = new ArrayList<Integer>();
		lst.add(1);
		lst.add(2);
		lst.add(3);
		lst.add(4);
		lst.add(5);
		

		Set<Set<Integer>> pairSet = new HashSet<Set<Integer>>();
		Set<Integer> pair = new HashSet<Integer>();

		for (int i = 0; i < lst.size(); i++) {
			for (int j = i + 1; j < lst.size(); j++) {
				pair.add(lst.get(i));
				pair.add(lst.get(j));
			}
		}

	}

}
