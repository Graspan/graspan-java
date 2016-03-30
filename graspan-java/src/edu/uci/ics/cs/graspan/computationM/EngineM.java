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
	private static final Logger logger = GraspanLogger.getLogger("Engine");
	private ExecutorService computationExecutor;
	private long totalNewEdges;
	private long totalDupEdges;
	private long newEdgesInOne;
	private long newEdgesInTwo;

	// private String baseFileName;
	private int[] partsToLoad;
	public static boolean memFull;

	public static boolean premature_Terminate;

	public static Vertex[] vertices_prevIt = null;
	public static NewEdgesList[] newEdgeLists_prevIt = null;
	public List<LoadedVertexInterval> intervals_prevIt = null;
	public static ComputationSet[] compSets_prevIt = null;

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 * @throws IOException
	 */
	public void run() throws IOException {

		// get the num of processors
		int nThreads = 1;
		if (Runtime.getRuntime().availableProcessors() > nThreads) {
			nThreads = Runtime.getRuntime().availableProcessors();
		}
		nThreads = 1;

		computationExecutor = Executors.newFixedThreadPool(nThreads);
		// logger.info("Executing partition loader.");
		long t = System.currentTimeMillis();

		// 1. load partitions into memory
		LoaderM loader = new LoaderM();

		Scheduler scheduler = new Scheduler(AllPartitions.partAllocTable.length);

		while (!scheduler.shouldTerminate()) {
			partsToLoad = scheduler
					.schedulePartitionSimple(AllPartitions.partAllocTable.length);
//			partsToLoad = scheduler
//					.schedulePartitionEDC(AllPartitions.partAllocTable.length);
			logger.info("Scheduling Partitions : "
					+ Arrays.toString(partsToLoad));
			logger.info("Start loading partitions...");
			loader.loadParts(partsToLoad);
			// logger.info("Total time for loading partitions : " +
			// (System.currentTimeMillis() - t) + " ms");

			Vertex[] vertices;
			vertices = loader.getVertices();

			// INITIAL SETUP OF THE COMPUTATION SET (FOR THE FIRST ITERATION OF
			// A LOADED SET OF PARTITIONS)
			ComputationSet[] compSets = new ComputationSet[vertices.length];
			int[] nullEdgs = new int[0];
			byte[] nullVals = new byte[0];
			for (int i = 0; i < compSets.length; i++) {
				compSets[i] = new ComputationSet();
				compSets[i].setOldEdgs(nullEdgs);
				compSets[i].setOldVals(nullVals);
				compSets[i].setNewEdgs(vertices[i].getOutEdges());
				compSets[i].setNewVals(vertices[i].getOutEdgeValues());
				compSets[i].setOldUnewEdgs(vertices[i].getOutEdges());
				compSets[i].setOldUnewVals(vertices[i].getOutEdgeValues());
			}

			List<LoadedVertexInterval> intervals = null;
			EdgeComputerM[] edgeComputers = new EdgeComputerM[vertices.length];
			intervals = loader.getIntervals();

			// this only does a shallow copy
			List<LoadedVertexInterval> intervalsForScheduler = new ArrayList(
					intervals);
			scheduler.setLoadedIntervals(intervalsForScheduler);
			logger.info("\nLVI after loading : " + intervals);
			assert (vertices != null && vertices.length > 0);
			assert (intervals != null && intervals.size() > 0);

			logger.info("Start computation and edge addition...");
			t = System.currentTimeMillis();

			// MemUsageCheckThread job1 = new MemUsageCheckThread();
			// job1.start();

			// 2. do computation and add edges
			EdgeComputerM.setComputationSets(compSets);
			EdgeComputerM.setVertices(vertices);
			EdgeComputerM.setIntervals(intervals);
			doComputation(vertices, compSets, edgeComputers, intervals);
			logger.info("Finish computation...");
			logger.info("Computation and edge addition took: "
					+ (System.currentTimeMillis() - t) + " ms");
			// logger.info("VERTEX LENGTH: " + vertices.length);
			// for(int i = 0; i < vertices.length; i++) {
			// logger.info("" + vertices[i]);
			// logger.info("" + edgesLists[i]);
			// }

			logger.info("Start storing partitions...");
			// 3. process computed partitions
			int numPartsStart = AllPartitions.getPartAllocTab().length;
			RepartitioningData.initRepartioningVars();
			ComputedPartProcessorM.initRepartitionConstraints();
			ComputedPartProcessorM.processParts(vertices, compSets, intervals);
			int numPartsFinal = AllPartitions.getPartAllocTab().length;

			vertices_prevIt = vertices;
			intervals_prevIt = intervals;
			logger.info("\nLVI after computedPartProcessor saves partitions : "
					+ intervals);
			logger.info("\nLVI (scheduler) after computedPartProcessor saves partitions : "
					+ intervalsForScheduler);
			scheduler.setTerminationStatus();
			scheduler.updateSchedInfoPostRepart(numPartsFinal - numPartsStart,
					numPartsFinal);
		}

		computationExecutor.shutdown();
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private void doComputation(final Vertex[] vertices,
			final ComputationSet[] compSets,
			final EdgeComputerM[] edgeComputers,
			List<LoadedVertexInterval> intervals) {
		if (vertices == null || vertices.length == 0)
			return;

		final Object termationLock = new Object();
		final int chunkSize = 1 + vertices.length / 64;
		// final int chunkSize = 1 + vertices.length / 4;

		int iterationNo = 0;

		final int nWorkers = vertices.length / chunkSize + 1;
		logger.info("nWorkers " + nWorkers);

		final AtomicInteger countDown = new AtomicInteger(nWorkers);

		// previous iteration compset elements
		ComputationSet[] compSets_prevIt = null;

		newEdgesInOne = 0;
		newEdgesInTwo = 0;
		assert (intervals.size() == 2);
		final int indexStartForOne = intervals.get(0).getIndexStart();
		final int indexEndForOne = intervals.get(0).getIndexEnd();
		final int indexStartForTwo = intervals.get(1).getIndexStart();
		final int indexEndForTwo = intervals.get(1).getIndexEnd();

		do {
			totalNewEdges = 0;
			totalDupEdges = 0;
			countDown.set(nWorkers);
			iterationNo++;

			// if this is not the first iteration no.
			// SET THE COMPUTATION ELEMENTS TO THOSE OF PREVIOUS ITERATION
			int[] nullDeltaEdgs, nullOldUnewUdeltaEdgs;
			byte[] nullDeltaVals, nullOldUnewUdeltaVals;
			if (iterationNo > 1) {
				logger.info("Entered iteration no. " + iterationNo);
				for (int i = 0; i < compSets.length; i++) {
					compSets[i].setOldEdgs(compSets_prevIt[i].getOldUnewEdgs());
					compSets[i].setOldVals(compSets_prevIt[i].getOldUnewVals());
					compSets[i].setNewEdgs(compSets_prevIt[i].getDeltaEdgs());
					compSets[i].setNewVals(compSets_prevIt[i].getDeltaVals());
					compSets[i].setOldUnewEdgs(compSets_prevIt[i]
							.getOldUnewUdeltaEdgs());
					compSets[i].setOldUnewVals(compSets_prevIt[i]
							.getOldUnewUdeltaVals());
					nullDeltaEdgs = new int[0];
					nullOldUnewUdeltaEdgs = new int[0];
					nullDeltaVals = new byte[0];
					nullOldUnewUdeltaVals = new byte[0];
					compSets[i].setDeltaEdges(nullDeltaEdgs);
					compSets[i].setDeltaVals(nullDeltaVals);
					compSets[i].setOldUnewUdeltaEdgs(nullOldUnewUdeltaEdgs);
					compSets[i].setOldUnewUdeltaVals(nullOldUnewUdeltaVals);
				}
			}

			// Parallel updates
			for (int id = 0; id < nWorkers; id++) {
				final int currentId = id;
				final int chunkStart = currentId * chunkSize;
				final int chunkEnd = chunkStart + chunkSize;

				computationExecutor.submit(new Runnable() {

					public void run() {
						int threadUpdates = 0;
						int dups = 0;

						logger.info("in multithreaded portion - chunk start: "
								+ chunkStart + " ThreadNo:"
								+ Thread.currentThread().getId());

						try {
							int end = chunkEnd;
							if (end > vertices.length)
								end = vertices.length;

							// logger.info(vertices.length + " vertex length");

							for (int i = chunkStart; i < end; i++) {
								// each vertex is associated with a computation
								// set
								Vertex vertex = vertices[i];
								ComputationSet compSet = compSets[i];
								EdgeComputerM edgeComputer = edgeComputers[i];

								if (vertex != null
										&& vertex.getNumOutEdges() != 0) {

									if (edgeComputer == null) {
										edgeComputer = new EdgeComputerM(
												vertex, compSet);
										edgeComputers[i] = edgeComputer;
									}

									// get termination status for each vertex
									if (edgeComputer.getTerminateStatus())
										continue;

									edgeComputer.execUpdate();
									threadUpdates = edgeComputer
											.getNumNewEdges();
									dups = edgeComputer.getNumDupEdges();
									compSet = edgeComputer
											.getSrcComputationSet();

									// logger.info("\nWould this work: "
									// + Arrays.toString(compSet
									// .getDeltaEdgs())
									// + " for vertexId #"
									// + vertices[i].getVertexId()
									// + "\n"
									// + Arrays.toString(compSets[i]
									// .getDeltaEdgs()));

									// check if there are new edges added in
									// partition one and two
									if (i >= indexStartForOne
											&& i <= indexEndForOne)
										newEdgesInOne += threadUpdates;
									else if (i >= indexStartForTwo
											&& i <= indexEndForTwo)
										newEdgesInTwo += threadUpdates;

									// set termination status if nNewEdges == 0
									// for each vertex
									if (threadUpdates == 0)
										edgeComputer.setTerminateStatus(true);
									edgeComputer.setNumNewEdges(0);
									edgeComputer.setNumDupEdges(0);
								}
							}

						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							int pending = countDown.decrementAndGet();
							synchronized (termationLock) {
								totalNewEdges += threadUpdates;
								totalDupEdges += dups;
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

			compSets_prevIt = compSets;

			// test printing compsets at the end of each iteration
			// for (int i = 0; i < compSets.length; i++) {
			// logger.info("Old Edges of compSet[" + 21 + "] for vid "
			// + vertices[21].getVertexId() + " "
			// + Arrays.toString(compSets[21].getOldEdgs()));
			// // }
			// // for (int i = 0; i < compSets.length; i++) {
			// logger.info("New Edges of compSet[" + 21 + "] for vid "
			// + vertices[21].getVertexId() + " "
			// + Arrays.toString(compSets[21].getNewEdgs()));
			// // }
			// // for (int i = 0; i < compSets.length; i++) {
			// logger.info("OldUNew Edges of compSet[" + 21 + "] for vid "
			// + vertices[21].getVertexId() + " "
			// + Arrays.toString(compSets[21].getOldUnewEdgs()));
			// // }
			// // for (int i = 0; i < compSets.length; i++) {
			// logger.info("Delta Edges of compSet[" + 21 + "] for vid "
			// + vertices[21].getVertexId() + " "
			// + Arrays.toString(compSets[21].getDeltaEdgs()));
			// // }
			// // for (int i = 0; i < compSets.length; i++) {
			// logger.info("OldUnewUdelta Edges of compSet[" + 21 + "] for vid "
			// + vertices[21].getVertexId() + " "
			// + Arrays.toString(compSets[21].getOldUnewUdeltaEdgs()));
			// }

			logger.info("========total # new edges for this iteration: "
					+ totalNewEdges);

			// SET THE LOADED PARTITION DATA STRUCTURES (EDGES) TO RELEVANT
			// COMPONENTS FROM COMPUTATION SET
			// TODO: NEED TO TEST THIS
			for (int i = 0; i < vertices.length; i++) {
				vertices[i].setOutEdges(compSets[i].getOldUnewUdeltaEdgs());
				vertices[i]
						.setOutEdgeValues(compSets[i].getOldUnewUdeltaVals());
			}
			// logger.info("========total # dup edges for this iteration: "
			// + totalDupEdges);
		} while (totalNewEdges > 0);

		// set new edge added flag for scheduler
		if (newEdgesInOne > 0)
			intervals.get(0).setIsNewEdgeAdded(true);
		if (newEdgesInTwo > 0)
			intervals.get(1).setIsNewEdgeAdded(true);

	}

}