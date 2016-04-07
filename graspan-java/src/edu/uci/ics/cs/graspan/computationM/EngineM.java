package edu.uci.ics.cs.graspan.computationM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.ComputationSet;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.NewEdgesList;
import edu.uci.ics.cs.graspan.datastructures.RepartitioningData;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.scheduler.IScheduler;
import edu.uci.ics.cs.graspan.scheduler.Scheduler;
import edu.uci.ics.cs.graspan.scheduler.SchedulerInfo;
import edu.uci.ics.cs.graspan.support.GraspanLogger;
import edu.uci.ics.cs.graspan.support.MemUsageCheckThread;

/**
 * @author Kai Wang
 * 
 *         Created by Oct 8, 2015
 */
public class EngineM {
	private static final Logger logger = GraspanLogger.getLogger("EngineM");

	private ExecutorService computationExecutor;

	private long totalNewEdgs;
	private long totalNewEdgsForIteratn;

	private long newEdgesInOne;
	private long newEdgesInTwo;

	private int[] partsToLoad;

	public static boolean memFull;

	public static boolean premature_Terminate;

	public static Vertex[] vertices_prevIt;
	public static NewEdgesList[] newEdgeLists_prevIt;
	public List<LoadedVertexInterval> intervals_prevIt;
	public static ComputationSet[] compSets_prevIt;

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 * @throws IOException
	 */
	public void run() throws IOException {
		// -------------------------------------------------------------------------------
		// get the num of processors
		int nThreads = 1;
		if (Runtime.getRuntime().availableProcessors() > nThreads) {
			nThreads = Runtime.getRuntime().availableProcessors();
		}
		// TODO: REMOVE THIS LATER
		nThreads = 8;
		computationExecutor = Executors.newFixedThreadPool(nThreads);

		// -------------------------------------------------------------------------------
		// instantiate loader
		LoaderM loader = new LoaderM();

		// instantiate scheduler
		Scheduler scheduler = new Scheduler(AllPartitions.partAllocTable.length);

		int roundNo = 0;
		while (!scheduler.shouldTerminate()) {
			roundNo++;
			logger.info("STARTING ROUND NO #" + roundNo);

			partsToLoad = scheduler.schedulePartitionSimple(AllPartitions.partAllocTable.length);
			// partsToLoad = scheduler.schedulePartitionEDC(AllPartitions.partAllocTable.length);
			logger.info("Scheduling Partitions : " + Arrays.toString(partsToLoad));
			logger.info("Start loading partitions...");

			loader.loadParts(partsToLoad);
			List<LoadedVertexInterval> intervals = loader.getIntervals();
			Vertex[] vertices = loader.getVertices();

			// send interval info to scheduler
			List<LoadedVertexInterval> intervalsForScheduler = new ArrayList<LoadedVertexInterval>(intervals);
			scheduler.setLoadedIntervals(intervalsForScheduler);
			logger.info("\nLVI after loading : " + intervals);
			assert (vertices != null && vertices.length > 0);
			assert (intervals != null && intervals.size() > 0);

			// for debugging
			// printSrcVerticesForDebugging(vertices);
			

			// *************************************************************************************************
			// computation
			ComputationSet[] compSets = new ComputationSet[vertices.length];
			for (int i = 0; i < compSets.length; i++) {
				compSets[i] = new ComputationSet();
				compSets[i].setNewEdgs(vertices[i].getOutEdges());
				compSets[i].setNewVals(vertices[i].getOutEdgeValues());
				compSets[i].setOldUnewEdgs(vertices[i].getOutEdges());
				compSets[i].setOldUnewVals(vertices[i].getOutEdgeValues());
			}

			logger.info("Finished initialization of CompSets and EdgeComputers");

			logger.info("Start computation and edge addition...");
			long t = System.currentTimeMillis();

			// MemUsageCheckThread job1 = new MemUsageCheckThread();
			// job1.start();

			// do computation and add edges
			computeForOneRound(vertices, compSets, intervals);

			logger.info("Finish computation for one round");
			logger.info("Computation and edge addition took: " + (System.currentTimeMillis() - t) + " ms");
			

			// *************************************************************************************************
			// post-processing: repartitioning
			logger.info("Start storing partitions...");
			int numPartsStart = AllPartitions.getPartAllocTab().length;
			RepartitioningData.initRepartioningVars();

			ComputedPartProcessorM.initRepartitionConstraints();
			ComputedPartProcessorM.processParts(vertices, compSets, intervals);
			int numPartsFinal = AllPartitions.getPartAllocTab().length;

			vertices_prevIt = vertices;
			intervals_prevIt = intervals;
			logger.info("\nLVI after computedPartProcessor saves partitions : " + intervals);
			logger.info("\nLVI (scheduler) after computedPartProcessor saves partitions : " + intervalsForScheduler);

			scheduler.setTerminationStatus();
			scheduler.updateSchedInfoPostRepart(numPartsFinal - numPartsStart, numPartsFinal);

			// //for debugging
			// printSrcVerticesForDebugging(vertices);
		}

		computationExecutor.shutdown();
	}

	/**
	 * for debugging only
	 * 
	 * @param vertices
	 */
	private void printSrcVerticesForDebugging(Vertex[] vertices) {
		String s = "";
		for (int i = 0; i < vertices.length; i++) {
			s = s + " " + vertices[i].getVertexId();
		}
		logger.info("All vertices in memory just after loading: \n" + s);
	}

	/**
	 * Computation for one round: add edges for two loaded partitions until no
	 * edges are added
	 * 
	 * @param vertices
	 * @param compSets
	 * @param edgeComputers
	 * @param intervals
	 */
	private void computeForOneRound(final Vertex[] vertices, final ComputationSet[] compSets, List<LoadedVertexInterval> intervals) {
		if (vertices == null || vertices.length == 0)
			return;

		newEdgesInOne = 0;
		newEdgesInTwo = 0;

		// initiate lock
		final Object termationLock = new Object();

		// set chunk size
		final int chunkSize = 1 + vertices.length / 64;
		final int nWorkers = vertices.length / chunkSize + 1;
		logger.info("nWorkers " + nWorkers);

		// get index for two partitions
		assert (intervals.size() == 2);
		final int indexStartForOne = intervals.get(0).getIndexStart();
		final int indexEndForOne = intervals.get(0).getIndexEnd();
		final int indexStartForTwo = intervals.get(1).getIndexStart();
		final int indexEndForTwo = intervals.get(1).getIndexEnd();

		int iterationNo = 0;
		do {
			iterationNo++;
			totalNewEdgsForIteratn = 0;

			logger.info("Entered iteration no. " + iterationNo);

			// parallel computation for one iteration
			parallelComputationForOneIteration(termationLock, chunkSize, nWorkers, vertices, compSets, intervals, 
					indexStartForOne, indexEndForOne, indexStartForTwo, indexEndForTwo);

			// for debugging: print compsets information at the end of each iteration
			// printCompSetsInfo(vertices, compSets);

			// update the number of total new edges
			this.totalNewEdgs += totalNewEdgsForIteratn;

			logger.info("========total # new edges for iteration #" + iterationNo + " is " + totalNewEdgsForIteratn);
			// logger.info("========total # dup edges for this iteration: " + totalDupEdges);
			
			
			assert(compSets.length == vertices.length);
			for (int i = 0; i < compSets.length; i++) {
				//resulting edges after one iteration
				vertices[i].setOutEdges(compSets[i].getOldUnewUdeltaEdgs());
				vertices[i].setOutEdgeValues(compSets[i].getOldUnewUdeltaVals());
				
				// update compsets before next iteration
				compSets[i].setOldEdgs(compSets[i].getOldUnewEdgs());
				compSets[i].setOldVals(compSets[i].getOldUnewVals());
				compSets[i].setNewEdgs(compSets[i].getDeltaEdgs());
				compSets[i].setNewVals(compSets[i].getDeltaVals());
				compSets[i].setOldUnewEdgs(compSets[i].getOldUnewUdeltaEdgs());
				compSets[i].setOldUnewVals(compSets[i].getOldUnewUdeltaVals());
			}

		} 
		while (totalNewEdgsForIteratn > 0);

		// set new edge added flag for scheduler
		if (newEdgesInOne > 0)
			intervals.get(0).setIsNewEdgeAdded(true);
		if (newEdgesInTwo > 0)
			intervals.get(1).setIsNewEdgeAdded(true);

	}

	/**
	 * @param termationLock
	 * @param chunkSize
	 * @param nWorkers
	 * @param vertices
	 * @param compSets
	 * @param intervals 
	 * @param indexStartForOne
	 * @param indexEndForOne
	 * @param indexStartForTwo
	 * @param indexEndForTwo
	 */
	private void parallelComputationForOneIteration(final Object termationLock, final int chunkSize, final int nWorkers,
			final Vertex[] vertices, final ComputationSet[] compSets, 
			final List<LoadedVertexInterval> intervals, final int indexStartForOne, final int indexEndForOne, final int indexStartForTwo,
			final int indexEndForTwo) {

		final AtomicInteger countDown = new AtomicInteger(nWorkers);

		// Parallel updates to finish all the workers' tasks as one iteration
		for (int id = 0; id < nWorkers; id++) {
			final int currentId = id;
			final int chunkStart = currentId * chunkSize;
			final int chunkEnd = chunkStart + chunkSize;

			computationExecutor.submit(new Runnable() {
				public void run() {
					long threadUpdates = 0;

					logger.info("in multithreaded portion - chunk start: " + chunkStart + " ThreadNo:"
							+ Thread.currentThread().getId());

					try {
						int end = chunkEnd;
						if (end > vertices.length)
							end = vertices.length;

						// do computation for one chunk
						for (int i = chunkStart; i < end; i++) {
							// each vertex is associated with a computation set
							Vertex vertex = vertices[i];

							if (vertex != null && vertex.getNumOutEdges() != 0) {
								// update edges for one src vertex
								threadUpdates = EdgeComputerM.execUpdate(i, compSets, intervals);
								
								// check if there are new edges added in partition one and two
								if (i >= indexStartForOne && i <= indexEndForOne)
									newEdgesInOne += threadUpdates;
								else if (i >= indexStartForTwo && i <= indexEndForTwo)
									newEdgesInTwo += threadUpdates;
							}
						}
					} 
					catch (Exception e) {
						e.printStackTrace();
					} 
					finally {
						int pending = countDown.decrementAndGet();
						synchronized (termationLock) {
							totalNewEdgsForIteratn += threadUpdates;
							if (pending == 0) {
								termationLock.notifyAll();
							}
						}
					}
				}

			});
		}

		synchronized (termationLock) {
			while (countDown.get() > 0) {
				try {
					termationLock.wait(1500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * for debugging: printing compsets at the end of each iteration
	 * 
	 * @param vertices
	 * @param compSets
	 */
	private void printCompSetsInfo(final Vertex[] vertices, final ComputationSet[] compSets) {
		for (int i = 0; i < compSets.length; i++) {
			logger.info("Old Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldEdgs()));
		}
		for (int i = 0; i < compSets.length; i++) {
			logger.info("New Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getNewEdgs()));
		}
		for (int i = 0; i < compSets.length; i++) {
			logger.info("OldUNew Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldUnewEdgs()));
		}
		for (int i = 0; i < compSets.length; i++) {
			logger.info("Delta Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getDeltaEdgs()));
		}
		for (int i = 0; i < compSets.length; i++) {
			logger.info("OldUnewUdelta Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldUnewUdeltaEdgs()));
		}
	}

	public long get_totalNewEdgs() {
		return totalNewEdgs;
	}

}