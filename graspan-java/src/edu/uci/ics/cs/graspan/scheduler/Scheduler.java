package edu.uci.ics.cs.graspan.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * @author Kai Wang
 * 
 *         Created by Oct 30, 2015
 */
public class Scheduler {
	private List<PartitionEdgeInfo> allEdgeInfo = new ArrayList<PartitionEdgeInfo>();
	private List<Long> partitionNumEdges = new ArrayList<Long>();
	private List<LoadedVertexInterval> intervals = null;
	private static final Logger logger = GraspanLogger.getLogger("Scheduler");

	// for temp use
	public static int counterOne = 0;
	public static int counterTwo = 1;

	/**
	 * Constructor 1. Only consider termination w/o scheduling
	 * 
	 * @param numParts
	 */
	public Scheduler(int numParts) {
		for (int i = 0; i < numParts; i++) {
			PartitionEdgeInfo edgeInfo = new PartitionEdgeInfo(i, numParts);
			allEdgeInfo.add(edgeInfo);
		}

		// //
		// Scheduler Output
		// StringBuilder result = new StringBuilder();
		// String NEW_LINE = System.getProperty("line.separator");
		// for (int i = 0; i < 50; i++) {
		// result.append(NEW_LINE + i + " : ");
		// for (int j = 0; j < 50; j++) {
		// result.append("(" + j + "," + SchedulerInfo.getEdgeDestCount()[i][j]
		// + ") ");
		// }
		// }
		// logger.info("Edge Dest Count" + result);
		// //
	}

	/**
	 * Constructor 2. Termination + scheduling
	 * 
	 * @param partSizes
	 * @param edgeDstCount
	 */
	public Scheduler(long[][] edgeDstCount) {
		if (SchedulerInfo.getPartSizes() == null || edgeDstCount == null)
			throw new IllegalArgumentException("Null parameter in scheduler!");

		for (int i = 0; i < SchedulerInfo.getPartSizes().length; i++) {
			partitionNumEdges.add(i, SchedulerInfo.getPartSizes()[i][1]);
		}

		for (int i = 0; i < edgeDstCount.length; i++) {
			PartitionEdgeInfo edgeInfo = new PartitionEdgeInfo(i,
					edgeDstCount[i]);
			allEdgeInfo.add(edgeInfo);
		}
	}

	public void setLoadedIntervals(List<LoadedVertexInterval> intervals) {
		this.intervals = intervals;
	}

	public List<LoadedVertexInterval> getLoadedIntervals() {
		return intervals;
	}


	/**
	 * Sequential Scheduler (Basic)
	 * 
	 * @param numOfPartitions
	 * @return
	 */
	public int[] schedulePartitionSimple(int numOfPartitions) {
		int[] scheduled = new int[2];

		// schedule two partitions every time
		scheduled[0] = counterOne;
		scheduled[1] = counterTwo++;

		if (counterTwo >= numOfPartitions) {
			counterOne++;
			counterTwo = counterOne + 1;
		}

		if (counterOne >= numOfPartitions - 1) {
			counterOne = 0;
			counterTwo = counterOne + 1;
		}
		return scheduled;
	}

	/**
	 * EDC-Based Scheduler
	 * 
	 * @return
	 */
	public int[] schedulePartitionEDC(int numOfPartitions) {
		int[] scheduled = new int[2];
		int partA = 0, partB = 0;
		long partA_Size = 0;
		long partB_Size = 0;
		double[][] edcPercentage = SchedulerInfo.getEdcPercentage();

		for (int i = 0; i < numOfPartitions; i++) {
			partA = i;

			// find size of partA
			for (int j = 0; j < SchedulerInfo.getPartSizes().length; j++) {
				if (SchedulerInfo.getPartSizes()[j][0] == partA) {
					partA_Size = SchedulerInfo.getPartSizes()[j][1];
					break;
				}
			}

			for (int j = 0; j < numOfPartitions; j++) {

				// OPTION 1-----
				edcPercentage[i][j] = (double) SchedulerInfo.getEdgeDestCount()[i][j] / partA_Size;
				// the remaining implementation needs to be changed if you want
				// to
				// implement this one
				// -------------

				// OPTION 2-----
				//TODO: don't use this for now, double check first
				// find size of partB
				// partB = j;
				// for (int k = 0; k < SchedulerInfo.getPartSizes().length; k++)
				// {
				// if (SchedulerInfo.getPartSizes()[k][0] == partB) {
				// partB_Size = SchedulerInfo.getPartSizes()[k][1];
				// break;
				// }
				// }
				//
				// edcPercentage[i][j] = (double)
				// SchedulerInfo.getEdgeDestCount()[i][j]
				// / partA_Size
				// + (double) SchedulerInfo.getEdgeDestCount()[j][i]
				// / partB_Size;
				// -------------
			}
		}

		// find max value
		double max = -1;
		int maxPartA = -1, maxPartB = -1;
		for (int i = 0; i < numOfPartitions; i++) {
			for (int j = 0; j < numOfPartitions; j++) {
				// logger.info("hello world" + edcPercentage[i][j] + " " + max);
				if (edcPercentage[i][j] > max && i != j) {
					List<Boolean> terminationInfoForOne = allEdgeInfo.get(i)
							.getTerminationInfo();
					if (terminationInfoForOne.get(j) == false) {
						max = edcPercentage[i][j];
						maxPartA = i;
						maxPartB = j;
					}
				}
			}
		}

		// schedule two partitions every time
		scheduled[0] = maxPartA;
		scheduled[1] = maxPartB;

		return scheduled;
	}

	/**
	 * 
	 * Description:check whether to be terminated. call it before every
	 * iteration.
	 * 
	 * @param:
	 * @return:boolean
	 */
	public boolean shouldTerminate() {
		int size = allEdgeInfo.size();

		for (int i = 0; i < allEdgeInfo.size(); i++) {
			int partitionId = allEdgeInfo.get(i).getPartitionId();
			List<Boolean> terminationInfo = allEdgeInfo.get(i)
					.getTerminationInfo();
			for (int j = (partitionId + 1); j < size; j++) {
				if (!terminationInfo.get(j))
					return false;
			}
		}

		// //
		// Scheduler Output
		// StringBuilder result = new StringBuilder();
		// String NEW_LINE = System.getProperty("line.separator");
		// for (int i = 0; i < 50; i++) {
		// result.append(NEW_LINE + i + " : ");
		// for (int j = 0; j < 50; j++) {
		// result.append("(" + j + "," + SchedulerInfo.getEdgeDestCount()[i][j]
		// + ") ");
		// }
		// }
		// logger.info("Edge Dest Count" + result);
		// //

		logger.info("=================Terminate!=================");
		return true;
	}

	/**
	 * 
	 * Description: set termination status every time after computation.
	 * 
	 * @param:
	 * @return:void
	 */
	public void setTerminationStatus() {
		logger.info("\nintervals : " + intervals);
		assert (intervals.size() == 2);

		int loadedPartitionOne = intervals.get(0).getPartitionId();
		boolean isNewEdgeAddedForOne = intervals.get(0).hasNewEdges();
		int loadedPartitionTwo = intervals.get(1).getPartitionId();
		boolean isNewEdgeAddedForTwo = intervals.get(1).hasNewEdges();

		logger.info("partitionId 1: " + loadedPartitionOne + ". New edges : "+ isNewEdgeAddedForOne);
		logger.info("partitionId 2: " + loadedPartitionTwo + ". New edges : "+ isNewEdgeAddedForTwo);

		List<Boolean> terminationInfoForOne = allEdgeInfo.get(
				loadedPartitionOne).getTerminationInfo();
		List<Boolean> terminationInfoForTwo = allEdgeInfo.get(
				loadedPartitionTwo).getTerminationInfo();

		if (isNewEdgeAddedForOne) {
			// set the row to false
			for (int i = 0; i < terminationInfoForOne.size(); i++)
				terminationInfoForOne.set(i, false);

			// set the column to false
			for (int i = 0; i < allEdgeInfo.size(); i++)
				allEdgeInfo.get(i).getTerminationInfo()
						.set(loadedPartitionTwo, false);
		}

		if (isNewEdgeAddedForTwo) {
			// set the row to false
			for (int i = 0; i < terminationInfoForTwo.size(); i++)
				terminationInfoForTwo.set(i, false);

			// set the column to false
			for (int i = 0; i < allEdgeInfo.size(); i++)
				allEdgeInfo.get(i).getTerminationInfo()
						.set(loadedPartitionOne, false);
		}

		terminationInfoForOne.set(loadedPartitionTwo, true);
		terminationInfoForTwo.set(loadedPartitionOne, true);
	}

	/**
	 * update termination status after repartitioning
	 * 
	 * @param numOfNewPartitions
	 * @param totalPartitions
	 */
	public void updateSchedInfoPostRepart(int numOfNewPartitions,
			int totalPartitions) {

		// for each of the old partitions, add "false" for each new partition
		for (PartitionEdgeInfo info : allEdgeInfo) {
			for (int i = 0; i < numOfNewPartitions; i++) {
				info.getTerminationInfo().add(Boolean.FALSE);
			}
		}

		// create records for each new partition created
		int numOfOldPartitions = totalPartitions - numOfNewPartitions;
		for (int i = 0; i < numOfNewPartitions; i++) {
			PartitionEdgeInfo newInfo = new PartitionEdgeInfo(
					numOfOldPartitions + i, totalPartitions);
			allEdgeInfo.add(newInfo);
		}
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");
		result.append(NEW_LINE + "number of partitions : " + allEdgeInfo.size());
		for (PartitionEdgeInfo info : allEdgeInfo)
			result.append(NEW_LINE + info.toString());

		return result.toString();
	}
}