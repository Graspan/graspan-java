package edu.uci.ics.cs.graspan.computationM;

import java.io.File;
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
import edu.uci.ics.cs.graspan.datastructures.PartitionQuerier;
import edu.uci.ics.cs.graspan.datastructures.RepartitioningData;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;
import edu.uci.ics.cs.graspan.scheduler.Scheduler;
import edu.uci.ics.cs.graspan.scheduler.SchedulerInfo;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

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
		
		GrammarChecker.loadGrammars(new File(GlobalParams.getBasefilename() + ".grammar"));
		
		// -------------------------------------------------------------------------------
		// get the num of processors
		int nThreads = 1;
		if (Runtime.getRuntime().availableProcessors() > nThreads) {
			nThreads = Runtime.getRuntime().availableProcessors();
		}
		nThreads = GlobalParams.getNumThreads();
		computationExecutor = Executors.newFixedThreadPool(nThreads);

		// -------------------------------------------------------------------------------
		// instantiate loader
		LoaderM loader = new LoaderM();

		// instantiate scheduler
		Scheduler scheduler = new Scheduler(AllPartitions.partAllocTable.length);
		Vertex[] vertices=null;
		int roundNo = 0;
		while (!scheduler.shouldTerminate()) {
			roundNo++;
			logger.info("STARTING ROUND NO #" + roundNo);

//			partsToLoad = scheduler.schedulePartitionSimple(AllPartitions.partAllocTable.length);
			partsToLoad = scheduler.schedulePartitionEDC(AllPartitions.partAllocTable.length);
			logger.info("Scheduling Partitions : " + Arrays.toString(partsToLoad));
			logger.info("Start loading partitions...");

			loader.loadParts(partsToLoad);
			List<LoadedVertexInterval> intervals = loader.getIntervals();
			vertices = loader.getVertices();

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
		logger.info("Total Num of New Edges: " + totalNewEdgs);
//		printSrcVerticesForDebugging(vertices);
		computationExecutor.shutdown();
	}

	/**
	 * for debugging only
	 * 
	 * @param vertices
	 */
	private void printSrcVerticesForDebugging(Vertex[] vertices) {
//		String s = "";
		int size=0;
		for (int i = 0; i < vertices.length; i++) {
			logger.info(vertices[i].getVertexId() + ": (dest ids) :" + Arrays.toString(vertices[i].getOutEdges()));
			logger.info(vertices[i].getVertexId() + ": (edge vals) :" + Arrays.toString(vertices[i].getOutEdgeValues()));
			size=size+vertices[i].getOutEdges().length;
		}
		logger.info("Total number of edges in vertices ds "+size);
//		logger.info("All vertices in memory just after loading: \n" + s);
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
			long t = System.currentTimeMillis();
			logger.info("Entered iteration no. " + iterationNo);

//			printCompSetsInfo(vertices,compSets);
			
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
				// resulting edges after one iteration
				vertices[i].setOutEdges(compSets[i].getOldUnewUdeltaEdgs());
				vertices[i].setOutEdgeValues(compSets[i].getOldUnewUdeltaVals());
				
				/*
				 * Update edge-dest-count and edge-dest-count-2way
				 */
				int srcV, destV, partA, partB;
				long[][] edc = SchedulerInfo.getEdgeDestCount();
				long[][] edcTwoWay = SchedulerInfo.getEdcTwoWay();
		
				//1. get source vertex partition Id
				 srcV = vertices[i].getVertexId();
				 partA = PartitionQuerier.findPartition(srcV);
				 
				 //2. scan each destination vertex id
				 for (int k = 0; k < compSets[i].getDeltaEdgs().length; k++)
				 {
					 //2.1.  get dest vertex partition Id
					 destV =compSets[i].getDeltaEdgs()[k];
					 partB = PartitionQuerier.findPartition(destV);
					 if (partB == -1)  // destination v does not lie in any partition
						 continue;
					 //2.2. increment entry in edc table
					 edc[partA][partB]++;
					 
					 //2.3. update edc-two-way
					 if (partA == partB)
							edcTwoWay[partA][partB] = edc[partA][partB];
					else
							edcTwoWay[partA][partB] = edc[partA][partB] + edc[partB][partA];
				 }

				 // update compsets before next iteration
				compSets[i].setOldEdgs(compSets[i].getOldUnewEdgs());
				compSets[i].setOldVals(compSets[i].getOldUnewVals());
				compSets[i].setNewEdgs(compSets[i].getDeltaEdgs());
				compSets[i].setNewVals(compSets[i].getDeltaVals());
				compSets[i].setOldUnewEdgs(compSets[i].getOldUnewUdeltaEdgs());
				compSets[i].setOldUnewVals(compSets[i].getOldUnewUdeltaVals());
			}
			
			logger.info("Finished iteration no. " + iterationNo + " took " + (System.currentTimeMillis() - t) / 1000 + " s");
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
		final Object counterLock = new Object();

		// Parallel updates to finish all the workers' tasks as one iteration
		for (int id = 0; id < nWorkers; id++) {
			final int currentId = id;
			final int chunkStart = currentId * chunkSize;
			final int chunkEnd = chunkStart + chunkSize;

			computationExecutor.submit(new Runnable() {
				public void run() {
					long threadUpdates = 0;

//					logger.info("in multithreaded portion - chunk start: " + chunkStart + " ThreadNo:"
//							+ Thread.currentThread().getId());

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
								synchronized(counterLock) {
									totalNewEdgsForIteratn += threadUpdates;
									// check if there are new edges added in partition one and two
									if (i >= indexStartForOne && i <= indexEndForOne)
										newEdgesInOne += threadUpdates;
									else if (i >= indexStartForTwo && i <= indexEndForTwo)
										newEdgesInTwo += threadUpdates;
								}
							}
						}
					} 
					catch (Exception e) {
						e.printStackTrace();
					} 
					finally {
						int pending = countDown.decrementAndGet();
						synchronized (termationLock) {
//							totalNewEdgsForIteratn += threadUpdates;
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
			if (vertices[i].getVertexId() == 4017) {
			logger.info("Old Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldEdgs()));
			logger.info("Old Values of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldVals()));
//		}
//		for (int i = 0; i < compSets.length; i++) {
			logger.info("New Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getNewEdgs()));
			logger.info("New Values of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getNewVals()));
//		}
//		for (int i = 0; i < compSets.length; i++) {
			logger.info("OldUNew Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldUnewEdgs()));
			logger.info("OldUNew Values of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldUnewVals()));
//		}
//		for (int i = 0; i < compSets.length; i++) {
			logger.info("Delta Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getDeltaEdgs()));
			logger.info("Delta Values of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getDeltaVals()));
//		}
//		for (int i = 0; i < compSets.length; i++) {
			logger.info("OldUnewUdelta Edges of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldUnewUdeltaEdgs()));
			logger.info("OldUnewUdelta Values of compSet[" + i + "] for vid " + vertices[i].getVertexId() + " "
					+ Arrays.toString(compSets[i].getOldUnewUdeltaVals()));
			}
		}
		
		
	}

	public long get_totalNewEdgs() {
		return totalNewEdgs;
	}

}