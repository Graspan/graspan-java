package edu.uci.ics.cs.graspan.computation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.datastructures.AllPartitions;
import edu.uci.ics.cs.graspan.datastructures.LoadedVertexInterval;
import edu.uci.ics.cs.graspan.datastructures.NewEdgesList;
import edu.uci.ics.cs.graspan.datastructures.RepartitioningData;
import edu.uci.ics.cs.graspan.datastructures.Vertex;
import edu.uci.ics.cs.graspan.scheduler.IScheduler;
import edu.uci.ics.cs.graspan.scheduler.Scheduler;
import edu.uci.ics.cs.graspan.support.GraspanLogger;

/**
 * @author Kai Wang
 *
 *         Created by Oct 8, 2015
 */
public class Engine {
	private static final Logger logger = GraspanLogger.getLogger("Engine");
	private ExecutorService computationExecutor;
	private long totalNewEdges;
	private long newEdgesInOne;
	private long newEdgesInTwo;
	// private String baseFileName;
	private int[] partsToLoad;
	private IScheduler scheduler;

	public static Vertex[] vertices_prevIt = null;
	public static NewEdgesList[] newEdgeLists_prevIt = null;
	public List<LoadedVertexInterval> intervals_prevIt = null;

	// public Engine(int[] partitionsToLoad) {
	//// this.baseFileName = baseFileName;
	// this.partsToLoad = partitionsToLoad;
	// }
	//
	// public Engine(IScheduler scheduler) {
	// this.scheduler = scheduler;
	// }

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
		// logger.info("Executing partition loader.");
		long t = System.currentTimeMillis();

		// 1. load partitions into memory
		Loader loader = new Loader();

		Scheduler scheduler = new Scheduler(AllPartitions.partAllocTable.length);

		while (!scheduler.shouldTerminate()) {
			partsToLoad = scheduler.schedulePartitionSimple(AllPartitions.partAllocTable.length);
			// logger.info("Scheduling Partitions : " +
			// Arrays.toString(partsToLoad));
			// logger.info("Start loading partitions...");
			loader.loadParts(partsToLoad);
			// logger.info("Total time for loading partitions : " +
			// (System.currentTimeMillis() - t) + " ms");

			Vertex[] vertices;
			NewEdgesList[] edgesLists;
			vertices = loader.getVertices();
			List<LoadedVertexInterval> intervals=null;
			EdgeComputer[] edgeComputers = new EdgeComputer[vertices.length];
			intervals = loader.getIntervals();
			scheduler.setLoadedIntervals(intervals);
			logger.info("\nintervals : " + intervals);
			assert(vertices != null && vertices.length > 0);
			assert(intervals != null && intervals.size() > 0);
			if (vertices_prevIt == null) {// if there was no previous iteration
				edgesLists = loader.getNewEdgeLists();
			} else {
				//use intervals_prevIt to set up edge lists;
				for (LoadedVertexInterval intvNew:intervals){
					for (LoadedVertexInterval intvOld:intervals){
						if (intvNew.getPartitionId()==intvOld.getPartitionId()){
							
//							newEdgeLists
						}
					}
				}
				edgesLists = newEdgeLists_prevIt;
			}
			// logger.info("VERTEX LENGTH: " + vertices.length);
			// logger.info("The vertices before setting degree after new
			// edges:");
			// logger.info(vertices[i]+"");
			// logger.info("New Edges: "+newEdgesLL[i]+"");

			// logger.info("Loading complete.");

			// TODO remove this!!!
			// System.exit(0);

			// logger.info("Start computation and edge addition...");
			t = System.currentTimeMillis();

			// 2. do computation and add edges
			EdgeComputer.setEdgesLists(edgesLists);
			EdgeComputer.setVertices(vertices);
			EdgeComputer.setIntervals(intervals);
			doComputation(vertices, edgesLists, edgeComputers, intervals);
			// logger.info("Finish computation...");
			// logger.info("Computation and edge addition took: " +
			// (System.currentTimeMillis() - t) + "ms");
			// logger.info("VERTEX LENGTH: " + vertices.length);
			// for(int i = 0; i < vertices.length; i++) {
			// logger.info("" + vertices[i]);
			// logger.info("" + edgesLists[i]);
			// }

			// logger.info("Start storing partitions...");
			// 3. process computed partitions
			int numPartsStart = AllPartitions.getPartAllocTab().length;
			RepartitioningData.initRepartioningVars();
			ComputedPartProcessor.initRepartitionConstraints();
			ComputedPartProcessor.processParts(vertices, edgesLists, intervals);
			int numPartsFinal = AllPartitions.getPartAllocTab().length;
			// logger.info("termination map before: " + scheduler.toString());

			for (int i = 0; i < vertices.length; i++) {
				logger.info("" + vertices[i]);
				logger.info("" + edgesLists[i]);
			}

			vertices_prevIt = vertices;
			newEdgeLists_prevIt = edgesLists;
			intervals_prevIt = intervals;
			scheduler.setTerminationStatus();
			// logger.info("termination map after: " + scheduler.toString());
			// scheduler.updateSchedulingInfo(numPartsFinal - numPartsStart,
			// numPartsFinal);
		}

		computationExecutor.shutdown();
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private void storePartitions() {

	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private void doComputation(final Vertex[] vertices, final NewEdgesList[] edgesLists,
			final EdgeComputer[] edgeComputers, List<LoadedVertexInterval> intervals) {
		if (vertices == null || vertices.length == 0)
			return;

		final Object termationLock = new Object();
		final int chunkSize = 1 + vertices.length / 64;

		final int nWorkers = vertices.length / chunkSize + 1;
		final AtomicInteger countDown = new AtomicInteger(nWorkers);

		newEdgesInOne = 0;
		newEdgesInTwo = 0;
		assert(intervals.size() == 2);
		final int indexStartForOne = intervals.get(0).getIndexStart();
		final int indexEndForOne = intervals.get(0).getIndexEnd();
		final int indexStartForTwo = intervals.get(1).getIndexStart();
		final int indexEndForTwo = intervals.get(1).getIndexEnd();

		do {
			// set readable index, for read and write concurrency
			// for current iteration, readable index points to the last new edge
			// in the previous iteration
			// which is readable for the current iteration
			setReadableIndex(edgesLists);

			totalNewEdges = 0;
			countDown.set(nWorkers);
			// Parallel updates
			for (int id = 0; id < nWorkers; id++) {
				final int currentId = id;
				final int chunkStart = currentId * chunkSize;
				final int chunkEnd = chunkStart + chunkSize;

				computationExecutor.submit(new Runnable() {

					public void run() {
						int threadUpdates = 0;

						try {
							int end = chunkEnd;
							if (end > vertices.length)
								end = vertices.length;

							for (int i = chunkStart; i < end; i++) {
								// each vertex is associated with an edgeList
								Vertex vertex = vertices[i];
								NewEdgesList edgeList = edgesLists[i];
								EdgeComputer edgeComputer = edgeComputers[i];

								if (vertex != null && vertex.getNumOutEdges() != 0) {
									if (edgeList == null) {
										edgeList = new NewEdgesList();
										edgesLists[i] = edgeList;
									}

									if (edgeComputer == null) {
										edgeComputer = new EdgeComputer(vertex, edgeList);
										edgeComputers[i] = edgeComputer;
									}

									// get termination status for each vertex
									if (edgeComputer.getTerminateStatus())
										continue;

									edgeComputer.execUpdate();
									threadUpdates = edgeComputer.getNumNewEdges();

									// check if there are new edges added in
									// partition one and two
									if (i >= indexStartForOne && i <= indexEndForOne)
										newEdgesInOne += threadUpdates;
									else if (i >= indexStartForTwo && i <= indexEndForTwo)
										newEdgesInTwo += threadUpdates;

									// set termination status if nNewEdges == 0
									// for each vertex
									if (threadUpdates == 0)
										edgeComputer.setTerminateStatus(true);
									edgeComputer.setNumNewEdges(0);
								}
							}

						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							int pending = countDown.decrementAndGet();
							synchronized (termationLock) {
								totalNewEdges += threadUpdates;
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
						logger.info("Waiting for execution to finish: countDown:" + countDown.get());
				}
			}
			// logger.info("========total new edges: " + totalNewEdges);
		} while (totalNewEdges > 0);

		// set new edge added flag for scheduler
		if (newEdgesInOne > 0)
			intervals.get(0).setIsNewEdgeAdded(true);
		if (newEdgesInTwo > 0)
			intervals.get(1).setIsNewEdgeAdded(true);
	}

	/**
	 * Description:
	 * 
	 * @param:
	 * @return:
	 */
	private void setReadableIndex(NewEdgesList[] edgesList) {
		if (edgesList == null || edgesList.length == 0)
			return;

		for (int i = 0; i < edgesList.length; i++) {
			NewEdgesList list = edgesList[i];
			if (list == null)
				continue;
			int size = list.getSize();
			if (size == 0)
				continue;
			list.setReadableSize(size);
			list.setReadableIndex(list.getIndex());
			list.setReadableLast(list.getLast());
		}
	}

}