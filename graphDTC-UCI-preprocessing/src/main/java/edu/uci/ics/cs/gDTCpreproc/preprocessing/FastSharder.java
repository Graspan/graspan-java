package edu.uci.ics.cs.gDTCpreproc.preprocessing;

import nom.tam.util.BufferedDataInputStream;

import java.io.*;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.zip.DeflaterOutputStream;

import edu.uci.ics.cs.gDTCpreproc.ChiFilenames;
import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.ChiVertex;
import edu.uci.ics.cs.gDTCpreproc.datablocks.BytesToValueConverter;
import edu.uci.ics.cs.gDTCpreproc.datablocks.ChiPointer;
import edu.uci.ics.cs.gDTCpreproc.datablocks.DataBlockManager;
import edu.uci.ics.cs.gDTCpreproc.datablocks.IntConverter;
import edu.uci.ics.cs.gDTCpreproc.engine.auxdata.VertexData;
import edu.uci.ics.cs.gDTCpreproc.io.CompressedIO;
import edu.uci.ics.cs.gDTCpreproc.shards.MemoryShard;
import edu.uci.ics.cs.gDTCpreproc.shards.SlidingShard;

/**
 * New version of sharder that requires predefined number of shards
 * and translates the vertex ids in order to randomize the order, thus
 * requiring no additional step to divide the number of edges for
 * each shard equally (it is assumed that probablistically the number
 * of edges is roughly even).
 *
 * Since the vertex ids are translated to internal-ids, you need to use
 * VertexIdTranslate class to obtain the original id-numbers.
 *
 * Usage:
 * <code>
 *     FastSharder sharder = new FastSharder(graphName, numShards, ....)
 *     sharder.shard(new FileInputStream())
 * </code>
 *
 * To use a pipe to feed a graph, use
 * <code>
 *     sharder.shard(System.in, "edgelist");
 * </code>
 *
 * <b>Note:</b> <a href="http://code.google.com/p/graphchi/wiki/EdgeListFormat">Edge list</a>
 * and <a href="http://code.google.com/p/graphchi/wiki/AdjacencyListFormat">adjacency list</a>
 * formats are supported.
 *
 * <b>Note:</b>If from and to vertex ids equal (applies only to edge list format), the line is assumed to contain vertex-value.
 *
 * @author Aapo Kyrola
 */
public class FastSharder <VertexValueType, EdgeValueType> {

    public enum GraphInputFormat {EDGELIST, ADJACENCY, MATRIXMARKET};

    private String baseFilename;
    private int numShards;
    private int initialIntervalLength;
    private VertexIdTranslate preIdTranslate;
    private VertexIdTranslate finalIdTranslate;

    private DataOutputStream[] shovelStreams;
    private DataOutputStream[] vertexShovelStreams;

    private int maxVertexId = 0;

    private int[] inDegrees;
    private int[] outDegrees;
    private boolean memoryEfficientDegreeCount = false;
    private long numEdges = 0;
    private boolean useSparseDegrees = false;
    private boolean allowSparseDegreesAndVertexData = false;

    private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;
    private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;

    private EdgeProcessor<EdgeValueType> edgeProcessor;
    private VertexProcessor<VertexValueType> vertexProcessor;


    private static final Logger logger = ChiLogger.getLogger("fast-sharder");

    /**
     * Constructor
     * @param baseFilename input-file
     * @param numShards the number of shards to be created
     * @param vertexProcessor user-provided function for translating strings to vertex value type
     * @param edgeProcessor user-provided function for translating strings to edge value type
     * @param vertexValConterter translator  byte-arrays to/from vertex-value
     * @param edgeValConverter   translator  byte-arrays to/from edge-value
     * @throws IOException  if problems reading the data
     */
    public FastSharder(String baseFilename, 
    				   int numShards,
                       VertexProcessor<VertexValueType> vertexProcessor,
                       EdgeProcessor<EdgeValueType> edgeProcessor,
                       BytesToValueConverter<VertexValueType> vertexValConterter,
                       BytesToValueConverter<EdgeValueType> edgeValConverter ) throws IOException {//ah46
        this.baseFilename = baseFilename;//ah46
        this.numShards = numShards;//ah46
        this.initialIntervalLength = Integer.MAX_VALUE / numShards;
        this.preIdTranslate = new VertexIdTranslate(this.initialIntervalLength, numShards);
        this.edgeProcessor = edgeProcessor;//ah46
        this.vertexProcessor = vertexProcessor;//ah46
        this.edgeValueTypeBytesToValueConverter = edgeValConverter;//ah46
        this.vertexValueTypeBytesToValueConverter = vertexValConterter;//ah46

        /**
         * In the first phase of processing, the edges are "shoveled" to
         * the corresponding shards. The interim shards are called "shovel-files",
         * and the final shards are created by sorting the edges in the shovel-files.
         * See processShovel()
         */
        shovelStreams = new DataOutputStream[numShards];
        vertexShovelStreams = new DataOutputStream[numShards];
        for(int i=0; i < numShards; i++) {
        	
        	/*
        	 * ah46. Empty "shovel" and "vertexshovel files are created"
        	 */
            shovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shovelFilename(i))));
            if (vertexProcessor != null) {
                vertexShovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(vertexShovelFileName(i))));
            }
        }

        /** Byte-array template used as a temporary value for performance (instead of
         *  always reallocating it).
         **/
        if (edgeValueTypeBytesToValueConverter != null) {//ah46
            valueTemplate =  new byte[edgeValueTypeBytesToValueConverter.sizeOf()];//ah46
        } else {//ah46
            valueTemplate = new byte[0];//ah46
        }
        if (vertexValueTypeBytesToValueConverter != null)//ah46
            vertexValueTemplate = new byte[vertexValueTypeBytesToValueConverter.sizeOf()];//ah46
    }

    /*
     * ah46. separate methods for filenames
     */
    private String shovelFilename(int i) {
        return baseFilename + ".shovel." + i;
    }

    private String vertexShovelFileName(int i) {
        return baseFilename + ".vertexshovel." + i;
    }


    /**
     * Adds an edge to the preprocessing.
     * @param from
     * @param to
     * @param edgeValueToken
     * @throws IOException
     */
    public void addEdge(int from, int to, String edgeValueToken) throws IOException {//ah46
        if (maxVertexId < from) maxVertexId = from;//ah46
        if (maxVertexId < to)  maxVertexId = to;//ah46

        /* If the from and to ids are same, this entry is assumed to contain value
           for the vertex, and it is passed to the vertexProcessor.
           ah46. we don't need this
         */
//        if (from == to) {
//            if (vertexProcessor != null && edgeValueToken != null) {
//                VertexValueType value = vertexProcessor.receiveVertexValue(from, edgeValueToken);
//                if (value != null) {
//                    addVertexValue(from % numShards, preIdTranslate.forward(from), value);
//                }
//            }
//            return;
//        }
        
        int preTranslatedIdFrom = preIdTranslate.forward(from);//ah46
        int preTranslatedTo = preIdTranslate.forward(to);//ah46
        
//        System.out.println(from);
//        System.out.println(preTranslatedIdFrom);
//        System.out.println(to);
//        System.out.println(preTranslatedTo);
        

        /*
         * TODO here edges are grouped by dest vertex id % number of partitions
         * we need to change this so that edges with the same group of source vertices are in the same partition
         */
        addToShovel(to % numShards, preTranslatedIdFrom, preTranslatedTo,
                (edgeProcessor != null ? edgeProcessor.receiveEdge(from, to, edgeValueToken) : null));//ah46
        
    }


    /*
     * valueTemplate is a byte array which is of size 4 for floats. This entire array will contain the edge value. 
     */
    private byte[] valueTemplate;//ah46
    
    
    private byte[] vertexValueTemplate;


    /**
     * Adds n edge to the shovel.  At this stage, the vertex-ids are "pretranslated"
     * to a temporary internal ids. In the last phase, each vertex-id is assigned its
     * final id. 
     * 
     * AH46. THE MAIN REASON FOR PRETRANSLATION: The pretranslation is required because at this point we do not know
     * the total number of vertices.
     * @param shard
     * @param preTranslatedIdFrom internal from-id
     * @param preTranslatedTo internal to-id
     * @param value -> the edge value
     * @throws IOException
     */
    private void addToShovel(int shard, int preTranslatedIdFrom, int preTranslatedTo,
                             EdgeValueType value) throws IOException {//ah46
        DataOutputStream strm = shovelStreams[shard];
        strm.writeLong(packEdges(preTranslatedIdFrom, preTranslatedTo));
        if (edgeValueTypeBytesToValueConverter != null) {
            edgeValueTypeBytesToValueConverter.setValue(valueTemplate, value);
        }
        strm.write(valueTemplate);
    }
    
   


    public boolean isAllowSparseDegreesAndVertexData() {
        return allowSparseDegreesAndVertexData;
    }

    /**
     * If set true, GraphChi will use sparse file for vertices and the degree data
     * if the number of edges is smaller than the number of vertices. Default false.
     * Note: if you use this, you probably want to set engine.setSkipZeroDegreeVertices(true)
     * @param allowSparseDegreesAndVertexData
     */
    public void setAllowSparseDegreesAndVertexData(boolean allowSparseDegreesAndVertexData) {
        this.allowSparseDegreesAndVertexData = allowSparseDegreesAndVertexData;
    }

    /**
     * Bit arithmetic for packing two 32-bit vertex-ids into one 64-bit long.
     * @param a
     * @param b
     * @return
     */
    static long packEdges(int a, int b) {//ah46
        return ((long) a << 32) + b;
    }

    static int getFirst(long l) {//ah46
        return  (int)  (l >> 32);
    }

    static int getSecond(long l) {//ah46
        return (int) (l & 0x00000000ffffffffl);
    }

    /**
     * Final processing after all edges have been received.
     * @throws IOException
     */
    public void process() throws IOException {
        /* Check if we have enough memory to keep track of
           vertex degree in memory. If not, we need to run a special
           graphchi-program to create the degree-file.
         */

        // Ad-hoc: require that degree vertices won't take more than 5th of memory
        memoryEfficientDegreeCount = Runtime.getRuntime().maxMemory() / 5 <  ((long) maxVertexId) * 8;

        if (memoryEfficientDegreeCount) {
            logger.info("Going to use memory-efficient, but slower, method to compute vertex degrees.");
        }

        if (!memoryEfficientDegreeCount) {
            inDegrees = new int[maxVertexId + numShards];
            outDegrees = new int[maxVertexId + numShards];
        }

        /**
         * Now when we have the total number of vertices known, we can
         * construct the final translator.
         */
        /*
         * ah46. (1 + maxVertexId) / numShards + 1 is the size of each vertex interval
         */
        finalIdTranslate = new VertexIdTranslate((1 + maxVertexId) / numShards + 1, numShards);

        /**
         * Store information on how to translate internal vertex id to the original id.
         */
        saveVertexTranslate();//ah46

        /**
         * Close / flush each shovel-file.
         */
        for(int i=0; i < numShards; i++) {//ah46
            shovelStreams[i].close();
        }
        shovelStreams = null;//ah46

        /**
         *  Store the vertex intervals.
         */
        writeIntervals();//ah46

        /**
         * Process each shovel to create a final shard.
         */
        for(int i=0; i<numShards; i++) {
            processShovel(i);
        }

        /**
         * If we have more vertices than edges, it makes sense to use sparse representation
         * for the auxilliary degree-data and vertex-data files.
         */
        if (allowSparseDegreesAndVertexData) {
            useSparseDegrees = (maxVertexId > numEdges) || "1".equals(System.getProperty("sparsedeg"));
        } else {
            useSparseDegrees = false;
        }
        logger.info("Use sparse output: " + useSparseDegrees);

        /**
         * Construct the degree-data file which stores the in- and out-degree
         * of each vertex. See edu.uci.ics.cs.gDTCpreproc.engine.auxdata.DegreeData
         */
        if (!memoryEfficientDegreeCount) {
            writeDegrees();
        } else {
            computeVertexDegrees();
        }

    }


    /**
     * Consteuct the degree-file if we had degrees computed in-memory,
     * @throws IOException
     */
    private void writeDegrees() throws IOException {
        DataOutputStream degreeOut = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(ChiFilenames.getFilenameOfDegreeData(baseFilename, useSparseDegrees))));
        for(int i=0; i<inDegrees.length; i++) {
            if (!useSparseDegrees)   {
                degreeOut.writeInt(Integer.reverseBytes(inDegrees[i]));
                degreeOut.writeInt(Integer.reverseBytes(outDegrees[i]));
            } else {
                if (inDegrees[i] + outDegrees[i] > 0) {
                    degreeOut.writeInt(Integer.reverseBytes(i));
                    degreeOut.writeInt(Integer.reverseBytes(inDegrees[i]));
                    degreeOut.writeInt(Integer.reverseBytes(outDegrees[i]));
                }
            }
        }
        degreeOut.close();
    }

    private void writeIntervals() throws IOException{
        FileWriter wr = new FileWriter(ChiFilenames.getFilenameIntervals(baseFilename, numShards));
        for(int j=1; j<=numShards; j++) {
            int a =(j * finalIdTranslate.getVertexIntervalLength() -1);
            wr.write(a + "\n");
            if (a > maxVertexId) {
                maxVertexId = a;
            }
        }
        wr.close();
    }

    private void saveVertexTranslate() throws IOException {//ah46
        FileWriter wr = new FileWriter(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards));
        wr.write(finalIdTranslate.stringRepresentation());
        wr.close();
    }



    /**
     * Converts a shovel-file into a shard.
     * @param shardNum
     * @throws IOException
     */
    private void processShovel(int shardNum) throws IOException {
        File shovelFile = new File(shovelFilename(shardNum));//ah46
        int sizeOf = (edgeValueTypeBytesToValueConverter != null ? edgeValueTypeBytesToValueConverter.sizeOf() : 0);

        long[] shoveled = new long[(int) (shovelFile.length() / (8 + sizeOf))];

        // TODO: improve
        if (shoveled.length > 500000000) {
            throw new RuntimeException("Too big shard size, shovel length was: " + shoveled.length + " max: " + 500000000);
        }
        byte[] edgeValues = new byte[shoveled.length * sizeOf];


        logger.info("Processing shovel " + shardNum);

        /**
         * Read the edges into memory.
         */
        BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(shovelFile));
        for(int i=0; i<shoveled.length; i++) {
            long l = in.readLong();
            int from = getFirst(l);
            int to = getSecond(l);
            in.readFully(valueTemplate);

            int newFrom = finalIdTranslate.forward(preIdTranslate.backward(from));
            int newTo = finalIdTranslate.forward(preIdTranslate.backward(to));
            shoveled[i] = packEdges(newFrom, newTo);

            /* Edge value */
            int valueIdx = i * sizeOf;
            System.arraycopy(valueTemplate, 0, edgeValues, valueIdx, sizeOf);
            if (!memoryEfficientDegreeCount) {
                inDegrees[newTo]++;
                outDegrees[newFrom]++;
            }
        }
        numEdges += shoveled.length;

        in.close();

        /* Delete the shovel-file */
        shovelFile.delete();

        logger.info("Processing shovel " + shardNum + " ... sorting");

        /* Sort the edges */
        sortWithValues(shoveled, edgeValues, sizeOf);  // The source id is  higher order, so sorting the longs will produce right result

        logger.info("Processing shovel " + shardNum + " ... writing shard");


        /*
         Now write the final shard in a compact form. Note that there is separate shard
         for adjacency and the edge-data. The edge-data is split and stored into 4-megabyte compressed blocks.
         */

        /**
         * Step 1: ADJACENCY SHARD
         */
        File adjFile = new File(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards));
        DataOutputStream adjOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(adjFile)));
        File indexFile = new File(adjFile.getAbsolutePath() + ".index");
        DataOutputStream indexOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
        int curvid = 0;
        int istart = 0;
        int edgeCounter = 0;
        int lastIndexFlush = 0;
        int edgesPerIndexEntry = 4096; // Tuned for fast shard queries

        for(int i=0; i <= shoveled.length; i++) {
            int from = (i < shoveled.length ? getFirst(shoveled[i]) : -1);

            if (from != curvid) {
                /* Write index */
                if (edgeCounter - lastIndexFlush >= edgesPerIndexEntry) {
                    indexOut.writeInt(curvid);
                    indexOut.writeInt(adjOut.size());
                    indexOut.writeInt(edgeCounter);
                    lastIndexFlush = edgeCounter;
                }

                int count = i - istart;

                if (count > 0) {
                    if (count < 255) {
                        adjOut.writeByte(count);
                    } else {
                        adjOut.writeByte(0xff);
                        adjOut.writeInt(Integer.reverseBytes(count));
                    }
                }
                for(int j=istart; j<i; j++) {
                    adjOut.writeInt(Integer.reverseBytes(getSecond(shoveled[j])));
                    edgeCounter++;
                }

                istart = i;

                // Handle zeros
                if (from != (-1)) {
                    if (from - curvid > 1 || (i == 0 && from > 0)) {
                        int nz = from - curvid - 1;
                        if (i ==0 && from >0) nz = from;
                        do {
                            adjOut.writeByte(0);
                            nz--;
                            int tnz = Math.min(254, nz);
                            adjOut.writeByte(tnz);
                            nz -= tnz;
                        } while (nz > 0);
                    }
                }
                curvid = from;
            }
        }
        adjOut.close();
        indexOut.close();



        /**
         * Step 2: EDGE DATA
         */

        /* Create compressed edge data directories */
        if (sizeOf > 0) {
            int blockSize = ChiFilenames.getBlocksize(sizeOf);
            String edataFileName = ChiFilenames.getFilenameShardEdata(baseFilename, new BytesToValueConverter() {
                @Override
                public int sizeOf() {
                    return edgeValueTypeBytesToValueConverter.sizeOf();
                }

                @Override
                public Object getValue(byte[] array) {
                    return null;
                }

                @Override
                public void setValue(byte[] array, Object val) {
                }
            }, shardNum, numShards);
            File edgeDataSizeFile = new File(edataFileName + ".size");
            File edgeDataDir = new File(ChiFilenames.getDirnameShardEdataBlock(edataFileName, blockSize));
            if (!edgeDataDir.exists()) edgeDataDir.mkdir();

            long edatasize = shoveled.length * edgeValueTypeBytesToValueConverter.sizeOf();
            FileWriter sizeWr = new FileWriter(edgeDataSizeFile);
            sizeWr.write(edatasize + "");
            sizeWr.close();

            /* Create compressed blocks */
            int blockIdx = 0;
            int edgeIdx= 0;
            for(long idx=0; idx < edatasize; idx += blockSize) {
                File blockFile = new File(ChiFilenames.getFilenameShardEdataBlock(edataFileName, blockIdx, blockSize));
                OutputStream blockOs = (CompressedIO.isCompressionEnabled() ?
                        new DeflaterOutputStream(new BufferedOutputStream(new FileOutputStream(blockFile))) :
                        new FileOutputStream(blockFile));
                long len = Math.min(blockSize, edatasize - idx);
                byte[] block = new byte[(int)len];

                System.arraycopy(edgeValues, edgeIdx * sizeOf, block, 0, block.length);
                edgeIdx += len / sizeOf;

                blockOs.write(block);
                blockOs.close();
                blockIdx++;
            }

            assert(edgeIdx == edgeValues.length);
        }
    }

    private static Random random = new Random();



    // http://www.algolist.net/Algorithms/Sorting/Quicksort
    // TODO: implement faster
    private static int partition(long arr[], byte[] values, int sizeOf, int left, int right)
    {
        int i = left, j = right;
        long tmp;
        long pivot = arr[left + random.nextInt(right - left + 1)];
        byte[] valueTemplate = new byte[sizeOf];

        while (i <= j) {
            while (arr[i] < pivot)
                i++;
            while (arr[j] > pivot)
                j--;
            if (i <= j) {
                tmp = arr[i];

                /* Swap */
                System.arraycopy(values, j * sizeOf, valueTemplate, 0, sizeOf);
                System.arraycopy(values, i * sizeOf, values, j * sizeOf, sizeOf);
                System.arraycopy(valueTemplate, 0, values, i * sizeOf, sizeOf);

                arr[i] = arr[j];
                arr[j] = tmp;
                i++;
                j--;
            }
        }

        return i;
    }

    static void quickSort(long arr[], byte[] values, int sizeOf, int left, int right) {//ah46
        if (left < right) {
            int index = partition(arr, values, sizeOf, left, right);
            if (left < index - 1)
                quickSort(arr, values, sizeOf, left, index - 1);
            if (index < right)
                quickSort(arr, values, sizeOf, index, right);
        }
    }


    public static void sortWithValues(long[] shoveled, byte[] edgeValues, int sizeOf) {//ah46
        quickSort(shoveled, edgeValues, sizeOf, 0, shoveled.length - 1);
    }


    /**
     * Execute sharding by reading edges from a inputstream
     * @param inputStream
     * @param format graph input format
     * @throws IOException
     */
    public void shard(InputStream inputStream, GraphInputFormat format) throws IOException {//ah46
        BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));//ah46
        String ln;//ah46
        long lineNum = 0;//ah46
       
        
            while ((ln = ins.readLine()) != null) {//ah46
                if (ln.length() > 2 && !ln.startsWith("#")) {//ah46
                    lineNum++;//ah46
                    if (lineNum % 100000 == 0) logger.info("Reading line: " + lineNum);//ah46
                    String[] tok = ln.split("\t");//ah46
                   
                    if (tok.length == 1) tok = ln.split(" ");//ah46

                    if (tok.length > 1) {
                        if (format == GraphInputFormat.EDGELIST) {
                        /* Edge list: <src> <dst> <value> */
                            if (tok.length == 2) {
                                this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), null);//ah46
                            } else if (tok.length == 3) {
                                this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), tok[2]);
                            }
                        }  else {
                            throw new IllegalArgumentException("Please specify graph input format");
                        }
                    }
                }
            }
            
            //the vertex shovels have been created after above
            
      //  this.process();
    }

    /**
     * Shard a graph
     * @param inputStream
     * @param format "edgelist" or "adjlist" / "adjacency"
     * @throws IOException
     */
    public void shard(InputStream inputStream, String format) throws IOException {//ah46
        if (format == null || format.equals("edgelist")) {
            shard(inputStream, GraphInputFormat.EDGELIST);
        }
    }

    /**
     * Shard an input graph with edge list format.
     * @param inputStream
     * @throws IOException
     */
    public void shard(InputStream inputStream) throws IOException {//ah46
        shard(inputStream, GraphInputFormat.EDGELIST);
    }

    /**
     * Compute vertex degrees by running a special graphchi program.
     * This is done only if we do not have enough memory to keep track of
     * vertex degrees in-memory.
     */
    private void computeVertexDegrees() {
        try {
            logger.info("Use sparse degrees: " + useSparseDegrees);

            DataOutputStream degreeOut = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(ChiFilenames.getFilenameOfDegreeData(baseFilename, useSparseDegrees))));


            SlidingShard[] slidingShards = new SlidingShard[numShards];
            for(int p=0; p < numShards; p++) {
                int intervalSt = p * finalIdTranslate.getVertexIntervalLength();
                int intervalEn = (p + 1) * finalIdTranslate.getVertexIntervalLength() - 1;

                slidingShards[p] = new SlidingShard(null, ChiFilenames.getFilenameShardsAdj(baseFilename, p, numShards),
                        intervalSt, intervalEn);
                slidingShards[p].setOnlyAdjacency(true);
            }

            int SUBINTERVAL = 2000000;
            ExecutorService parallelExecutor = Executors.newFixedThreadPool(4);

            for(int p=0; p < numShards; p++) {
                logger.info("Degree computation round " + p + " / " + numShards);
                int intervalSt = p * finalIdTranslate.getVertexIntervalLength();
                int intervalEn = (p + 1) * finalIdTranslate.getVertexIntervalLength() - 1;

                MemoryShard<Float> memoryShard = new MemoryShard<Float>(null, ChiFilenames.getFilenameShardsAdj(baseFilename, p, numShards),
                        intervalSt, intervalEn);
                memoryShard.setOnlyAdjacency(true);


                for(int subIntervalSt=intervalSt; subIntervalSt < intervalEn; subIntervalSt += SUBINTERVAL) {
                    int subIntervalEn = subIntervalSt + SUBINTERVAL - 1;
                    if (subIntervalEn > intervalEn) subIntervalEn = intervalEn;
                    ChiVertex[] verts = new ChiVertex[subIntervalEn - subIntervalSt + 1];
                    for(int i=0; i < verts.length; i++) {
                        verts[i] = new ChiVertex(i + subIntervalSt, null);
                    }

                    memoryShard.loadVertices(subIntervalSt, subIntervalEn, verts, false, parallelExecutor);
                    for(int i=0; i < numShards; i++) {
                        if (i != p) {
                            slidingShards[i].readNextVertices(verts, subIntervalSt, true);
                        }
                    }

                    for(int i=0; i < verts.length; i++) {
                        if (!useSparseDegrees) {
                            degreeOut.writeInt(Integer.reverseBytes(verts[i].numInEdges()));
                            degreeOut.writeInt(Integer.reverseBytes(verts[i].numOutEdges()));
                        } else {
                            if (verts[i].numEdges() > 0 ){
                                degreeOut.writeInt(Integer.reverseBytes(subIntervalSt + i));
                                degreeOut.writeInt(Integer.reverseBytes(verts[i].numInEdges()));
                                degreeOut.writeInt(Integer.reverseBytes(verts[i].numOutEdges()));
                            }
                        }
                    }
                }
            }
            parallelExecutor.shutdown();
            degreeOut.close();
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

}
