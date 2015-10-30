package edu.uci.ics.cs.gdtc.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.cs.gdtc.data.LoadedVertexInterval;
import edu.uci.ics.cs.gdtc.data.Vertex;
import edu.uci.ics.cs.gdtc.edgecomputer.EdgeComputer;
import edu.uci.ics.cs.gdtc.edgecomputer.NewEdgesList;
import edu.uci.ics.cs.gdtc.edgecomputer.PartitionLoader;
import edu.uci.ics.cs.gdtc.support.GDTCLogger;


/**
 * @author Kai Wang
 *
 * Created by Oct 8, 2015
 */
public class Engine {
	private static final Logger logger = GDTCLogger.getLogger("graphdtc engine");
	private ExecutorService computationExecutor;
	private long totalNewEdges;
	private String baseFileName;
	private int[] partitionsToLoad;
	
	public Engine(String baseFileName, int[] partitionsToLoad) {
		this.baseFileName = baseFileName;
		this.partitionsToLoad = partitionsToLoad;
	}
	
	/**
	 * Description:
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
        
		logger.info("Loading Partitions...");
		long t = System.currentTimeMillis();
		
		// 1. load partitions into memory
		PartitionLoader loader = new PartitionLoader();
		loader.loadParts(baseFileName, partitionsToLoad, 2);
//		loadPartitions(verticesFrom, verticesTo);
		logger.info("Load took: " + (System.currentTimeMillis() - t) + "ms");
//		GraphDTCVertex[] verticesFrom = loader.getVerticesFrom();
//		GraphDTCVertex[] verticesTo = loader.getVerticesTo();
		Vertex[] vertices = loader.getVertices();
		ArrayList<LoadedVertexInterval> intervals = loader.getIntervals();
		assert(vertices != null && vertices.length > 0);
		assert(intervals != null && intervals.size() > 0);
		
//		GraphDTCNewEdgesList[] edgesLists = new GraphDTCNewEdgesList[verticesFrom.length + verticesTo.length];
//		EdgeComputer[] edgeComputers = new EdgeComputer[verticesFrom.length + verticesTo.length];
		NewEdgesList[] edgesLists = new NewEdgesList[vertices.length];
		EdgeComputer[] edgeComputers = new EdgeComputer[vertices.length];
		
//		logger.info("VERTEX LENGTH: " + verticesFrom.length);
//		for(int i = 0; i < verticesFrom.length; i++) {
//			logger.info("" + verticesFrom[i]);
//			logger.info("" + edgesLists[i]);
//		}
		logger.info("VERTEX LENGTH: " + vertices.length);
		for(int i = 0; i < vertices.length; i++) {
			logger.info("" + vertices[i]);
			logger.info("" + edgesLists[i]);
		}
		
		logger.info("Finish...");
		logger.info("Starting computation and edge addition...");
		t = System.currentTimeMillis();
		
		// 2. do computation and add edges
		EdgeComputer.setEdgesLists(edgesLists);
//		EdgeComputer.setVerticesFrom(verticesFrom);
//		EdgeComputer.setVerticesTo(verticesTo);
		EdgeComputer.setVertices(vertices);
		EdgeComputer.setIntervals(intervals);
//		doComputation(verticesFrom, verticesTo, edgesLists, edgeComputers);
		doComputation(vertices, edgesLists, edgeComputers);
		logger.info("Computation and edge addition took: " + (System.currentTimeMillis() - t) + "ms");
		
//		logger.info("VERTEX LENGTH: " + verticesFrom.length);
//		for(int i = 0; i < verticesFrom.length; i++) {
//			logger.info("" + verticesFrom[i]);
//			logger.info("" + edgesLists[i]);
//		}
		logger.info("VERTEX LENGTH: " + vertices.length);
		for(int i = 0; i < vertices.length; i++) {
			logger.info("" + vertices[i]);
			logger.info("" + edgesLists[i]);
		}
//		
//		// 3. store partitions to disk
//		storePartitions();
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
	private void doComputation(final Vertex[] vertices, 
			final NewEdgesList[] edgesLists,
			final EdgeComputer[] edgeComputers) {
//		if(verticesFrom == null || verticesFrom.length == 0)
//			return;
//		
//		if(verticesTo == null || verticesTo.length == 0)
//			return;
		if(vertices == null || vertices.length == 0)
			return;
		
//		final GraphDTCVertex[] vertices = verticesFrom;
		final Object termationLock = new Object();
        final int chunkSize = 1 + vertices.length / 64;

        final int nWorkers = vertices.length / chunkSize + 1;
        final AtomicInteger countDown = new AtomicInteger(nWorkers);
        int i = 0;
        do {
        	// set readable index, for read and write concurrency
    		// for current iteration, readable index points to the last new edge in the previous iteration
    		// which is readable for the current iteration
    		setReadableIndex(edgesLists);
    		
        	totalNewEdges = 0;
        	countDown.set(nWorkers);
        	logger.info("======================ROUND : " + i++);
	        // Parallel updates
	        for(int id = 0; id < nWorkers; id++) {
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
	                        
	                        for(int i = chunkStart; i < end; i++) {
	                        	// each vertex is associated with an edgeList
	                            Vertex vertex = vertices[i];
	                            NewEdgesList edgeList = edgesLists[i];
	                            EdgeComputer edgeComputer = edgeComputers[i];
	                            
	                            if (vertex != null && vertex.getNumOutEdges() != 0) {
	                            	if(edgeList == null) {
	                            		edgeList = new NewEdgesList();
	                            		edgesLists[i] = edgeList;
	                            	}
	                            	
	                            	if(edgeComputer == null) {
	                            		edgeComputer = new EdgeComputer(vertex, edgeList);
	                            		edgeComputers[i] = edgeComputer;
	                            	}
	                            	
	                            	// get termination status for each vertex
	                            	if(edgeComputer.getTerminateStatus())
	                            		continue;
	                            	
	                                edgeComputer.execUpdate();
	                                threadUpdates = edgeComputer.getNumNewEdges();
	                                
	                                // set termination status if nNewEdges == 0 for each vertex
	                                if(threadUpdates == 0)
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
	            while(countDown.get() > 0) {
	                try {
	                	termationLock.wait(1500);
	                } catch (InterruptedException e) {
	                    e.printStackTrace();
	                }
	                
	                if (countDown.get() > 0) 
	                	logger.info("Waiting for execution to finish: countDown:" + countDown.get());
	            }
	        }
	        
        } while(totalNewEdges > 0);
    }
	

	/**
	 * Description:
	 * @param:
	 * @return:
	 */
	private void setReadableIndex(NewEdgesList[] edgesList) {
		if(edgesList == null || edgesList.length == 0)
			return;
		
		for(int i = 0; i < edgesList.length; i++) {
			NewEdgesList list = edgesList[i];
			if(list == null)
				continue;
			int size = list.getSize();
			if(size == 0)
				continue;
			list.setReadableSize(size);
			int index = list.getIndex();
			list.setReadableIndex(index);
		}
	}

}