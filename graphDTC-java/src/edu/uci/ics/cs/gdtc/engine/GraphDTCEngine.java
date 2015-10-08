package edu.uci.ics.cs.gdtc.engine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import edu.uci.ics.gdtc.GraphDTCLogger;
import edu.uci.ics.gdtc.GraphDTCVertex;


/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class GraphDTCEngine {
	private static final Logger logger = GraphDTCLogger.getLogger("graphdtc engine");
	private ExecutorService computationExecutor;
	private long nUpdates;
	
	public GraphDTCEngine() {
		
	}
	
	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	public void run() {
		
		// get the num of processors
		int nThreads = 8;
        if (Runtime.getRuntime().availableProcessors() > nThreads) {
            nThreads = Runtime.getRuntime().availableProcessors();
        }
        
        computationExecutor = Executors.newFixedThreadPool(nThreads);
        
		int intervalEnd = 0;
		int intervalStart = 0;
		//TODO: get the num of vertices
		int nVertices = intervalEnd - intervalStart + 1;
		GraphDTCVertex[] vertices = new GraphDTCVertex[nVertices];
		
		logger.info("Loading Partitions...");
		long t = System.currentTimeMillis();
		// 1. load partitions into memory
		loadPartitions(vertices);
		logger.info("Load took: " + (System.currentTimeMillis() - t) + "ms");
		
		logger.info("Starting computation and edge addition...");
		t = System.currentTimeMillis();
		// 2. do computation and add edges
		doComputation(vertices);
		logger.info("Computation and edge addition took: " + (System.currentTimeMillis() - t) + "ms");
		
		// 3. store partitions to disk
		storePartitions();
	}

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void storePartitions() {
		
	}

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void doComputation(final GraphDTCVertex[] vertices) {
		if(vertices == null || vertices.length == 0)
			return;
		
		final Object termationLock = new Object();
        final int chunkSize = 1 + vertices.length / 64;

        final int nWorkers = vertices.length / chunkSize + 1;
        final AtomicInteger countDown = new AtomicInteger(1 + nWorkers);
        
        /* Parallel updates */
        for(int i = 0; i < nWorkers; i++) {
            final int currentId = i;
            final int chunkStart = currentId * chunkSize;
            final int chunkEnd = chunkStart + chunkSize;

            computationExecutor.submit(new Runnable() {

                public void run() {
                    int threadUpdates = 0;

                    try {
                        int end = chunkEnd;
                        if (end > vertices.length) end = vertices.length;
                        for(int i = chunkStart; i < end; i++) {
                            GraphDTCVertex vertex = vertices[i];
                            if (vertex != null) {
                            	threadUpdates++;
                                update(vertex);
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        int pending = countDown.decrementAndGet();
                        synchronized (termationLock) {
                            nUpdates += threadUpdates;
                            if (pending == 0) {
                            	termationLock.notifyAll();
                            }
                        }
                    }
                }
            });
        }
        
        synchronized (termationLock) {
            while(countDown.get() > 0) {
                try {
                	termationLock.wait(1500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (countDown.get() > 0) logger.info("Waiting for execution to finish: countDown:" + countDown.get());
            }
        }

    }
	

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void loadPartitions(GraphDTCVertex[] vertices) {
		
	}
	
	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void update(GraphDTCVertex vertex) {
		
	}
}
