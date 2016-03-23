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
	private int[] partsToLoad;
	private IScheduler scheduler;
	public static boolean memFull;

	public static boolean premature_Terminate;

	public static Vertex[] vertices_prevIt = null;
	public static NewEdgesList[] newEdgeLists_prevIt = null;
	public List<LoadedVertexInterval> intervals_prevIt = null;

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 * @throws IOException
	 */
	public void run() throws IOException {

		// get the num of processors
		int nThreads = 8;
		if (Runtime.getRuntime().availableProcessors() > nThreads) {
			nThreads = Runtime.getRuntime().availableProcessors();
		}

		computationExecutor = Executors.newFixedThreadPool(nThreads);
		long t = System.currentTimeMillis();

		// 1. load partitions into memory
		LoaderM loader = new LoaderM();

		Scheduler scheduler = new Scheduler(AllPartitions.partAllocTable.length);

		while (!scheduler.shouldTerminate()) {
			partsToLoad = scheduler
					.schedulePartitionEDC(AllPartitions.partAllocTable.length);
			logger.info("Scheduling Partitions : "
					+ Arrays.toString(partsToLoad));
			logger.info("Start loading partitions...");
			loader.loadParts(partsToLoad);
			// logger.info("Total time for loading partitions : " +
			// (System.currentTimeMillis() - t) + " ms");

			Vertex[] vertices;
			vertices = loader.getVertices();

			List<LoadedVertexInterval> intervals = null;
			intervals = loader.getIntervals();
			// this only does a shallow copy
			List<LoadedVertexInterval> intervalsForScheduler = new ArrayList(
					intervals);
			scheduler.setLoadedIntervals(intervalsForScheduler);
			logger.info("\nLVI after loading : " + intervals);
			assert (vertices != null && vertices.length > 0);
			assert (intervals != null && intervals.size() > 0);

			// initialize computation sets
			ComputationSet[] computationSets = new ComputationSet[vertices.length];

			for (int i = 0; i < vertices.length; i++) {
				computationSets[i] = new ComputationSet();
				computationSets[i].setNewTgts(vertices[i]
						.getOutEdges());
				computationSets[i].setNewTgtEdgeVals(vertices[i]
						.getOutEdgeValues());
			}

			EdgeComputerM[] edgeComputers = new EdgeComputerM[vertices.length];
			EdgeComputerM.setVertices(vertices);
			EdgeComputerM.setIntervals(intervals);
			EdgeComputerM.setComputationSets(computationSets);

			MemUsageCheckThread job1 = new MemUsageCheckThread();
			job1.start();
			logger.info("Start computation...");
			t = System.currentTimeMillis();

			doComputation(vertices, computationSets, edgeComputers, intervals);
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
			// ComputedPartProcessorM.processParts(vertices, edgesLists,
			// intervals);
			ComputedPartProcessorM.processParts(vertices, intervals);
			int numPartsFinal = AllPartitions.getPartAllocTab().length;

			vertices_prevIt = vertices;
			// newEdgeLists_prevIt = edgesLists;
			intervals_prevIt = intervals;
			logger.info("\nLVI after computedPartProcessor saves partitions : "
					+ intervals);
			logger.info("\nLVI (scheduler) after computedPartProcessor saves partitions : "
					+ intervalsForScheduler);
			scheduler.setTerminationStatus();
			// logger.info("termination map after: " + scheduler.toString());
			scheduler.updateSchedInfoPostRepart(numPartsFinal - numPartsStart,
					numPartsFinal);
			// logger.info("termination map after: " + scheduler.toString());
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
			final ComputationSet[] computationSets,
			final EdgeComputerM[] edgeComputers,
			List<LoadedVertexInterval> intervals) {
		if (vertices == null || vertices.length == 0)
			return;

		final Object termationLock = new Object();
		final int chunkSize = 1 + vertices.length / 64;
		// final int chunkSize = 1 + vertices.length / 4;

		final int nWorkers = vertices.length / chunkSize + 1;
		final AtomicInteger countDown = new AtomicInteger(nWorkers);

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
			// Parallel updates
			for (int id = 0; id < nWorkers; id++) {
				final int currentId = id;
				final int chunkStart = currentId * chunkSize;
				final int chunkEnd = chunkStart + chunkSize;

				computationExecutor.submit(new Runnable() {

					public void run() {
						int threadUpdates = 0;
						int dups = 0;

						try {
							int end = chunkEnd;
							if (end > vertices.length)
								end = vertices.length;

							// logger.info(vertices.length + " vertex length");

							for (int i = chunkStart; i < end; i++) {
								// each vertex is associated with a
								// ComputationSet
								Vertex vertex = vertices[i];
								ComputationSet computationSet = computationSets[i];
								EdgeComputerM edgeComputer = edgeComputers[i];

								if (vertex != null
										&& vertex.getNumOutEdges() != 0) {

									// initialize edgeComputer
									if (edgeComputer == null) {
										edgeComputer = new EdgeComputerM(
												vertex, computationSet);
										edgeComputers[i] = edgeComputer;
									}

									// get termination status for each vertex
									if (edgeComputer.getTerminateStatus())
										continue;

									edgeComputer.execUpdate();
									threadUpdates = edgeComputer
											.getNumNewEdges();
									dups = edgeComputer.getNumDupEdges();

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

					if (countDown.get() > 0)
						logger.info("Waiting for execution to finish: countDown:"
								+ countDown.get());
				}
			}
			logger.info("========total # new edges for this iteration: "
					+ totalNewEdges);
			logger.info("========total # dup edges for this iteration: "
					+ totalDupEdges);
		} while (totalNewEdges > 0);

		// set new edge added flag for scheduler
		if (newEdgesInOne > 0)
			intervals.get(0).setIsNewEdgeAdded(true);
		if (newEdgesInTwo > 0)
			intervals.get(1).setIsNewEdgeAdded(true);
	}

}