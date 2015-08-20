package edu.uci.ics.cs.gDTCpreproc.preprocessing;


import java.io.*;
import java.util.logging.Logger;

import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.datablocks.BytesToValueConverter;

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
 *     PartitionGenerator sharder = new PartitionGenerator(graphName, nParts, ....)
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
public class PartitionGenerator <VertexValueType, EdgeValueType> {

    public enum GraphInputFormat {EDGELIST, ADJACENCY, MATRIXMARKET};

    private String baseFilename;
    private int nParts;
    private int initialIntervalLength;
    private VertexIdTranslate preIdTranslate;
    private VertexIdTranslate finalIdTranslate;

    private DataOutputStream[] shovelStreams;

    private int maxVertexId = 0;


    private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;

    private EdgeProcessor<EdgeValueType> edgeProcessor;


    private static final Logger logger = ChiLogger.getLogger("fast-sharder");

    /**
     * Constructor
     * @param baseFilename input-file
     * @param nParts the number of partitions to be created
     * @param vertexProcessor user-provided function for translating strings to vertex value type
     * @param edgeProcessor user-provided function for translating strings to edge value type
     * @param vertexValConterter translator  byte-arrays to/from vertex-value
     * @param edgeValConverter   translator  byte-arrays to/from edge-value
     * @throws IOException  if problems reading the data
     */
    public PartitionGenerator(String baseFilename, 
    				   int numShards,
                       EdgeProcessor<EdgeValueType> edgeProcessor,
                       BytesToValueConverter<EdgeValueType> edgeValConverter ) throws IOException {//ah46
        this.baseFilename = baseFilename;//ah46
        this.nParts = numShards;//ah46
        this.initialIntervalLength = Integer.MAX_VALUE / numShards;
        this.preIdTranslate = new VertexIdTranslate(this.initialIntervalLength, numShards);
        this.edgeProcessor = edgeProcessor;//ah46
        this.edgeValueTypeBytesToValueConverter = edgeValConverter;//ah46

        /**
         * the edges are "shoveled" to
         * partitions. 
         */
        shovelStreams = new DataOutputStream[numShards];
        for(int i=0; i < numShards; i++) {
        	
        	/*
        	 * ah46. Empty "shovel" files are created"
        	 */
            shovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shovelFilename(i))));
        }

        /** Byte-array template used as a temporary value for performance (instead of
         *  always reallocating it).
         **/
        if (edgeValueTypeBytesToValueConverter != null) {//ah46
            valueTemplate =  new byte[edgeValueTypeBytesToValueConverter.sizeOf()];//ah46
        } else {//ah46
            valueTemplate = new byte[0];//ah46
        }
    }

    /*
     * ah46. separate methods for filenames
     */
    private String shovelFilename(int i) {
        return baseFilename + ".shovel." + i;
    }

    /**
     * Adds an edge to the preprocessing.
     * @param src
     * @param dest
     * @param edgeValueToken
     * @throws IOException
     */
    public void addEdge(int src, int dest, String edgeValueToken) throws IOException {//ah46
        if (maxVertexId < src) maxVertexId = src;//ah46
        if (maxVertexId < dest)  maxVertexId = dest;//ah46

        
        //doing translation
        int preTranslatedIdFrom = preIdTranslate.forward(src);//ah46
        int preTranslatedTo = preIdTranslate.forward(dest);//ah46
        

        /*
         * TODO here edges are grouped by dest vertex id % number of partitions
         * we need to change this so that edges with the same group of source vertices are in the same partition
         */
        addToShovel(dest % nParts, src, dest, (edgeProcessor != null ? edgeProcessor.receiveEdge(src, dest, edgeValueToken) : null));//ah46
        
    }


    /*
     * valueTemplate is a byte array which is of size 4 for floats. This entire array will contain the edge value. 
     */
    private byte[] valueTemplate;//ah46
    

    /**
     * Adds n edge to the shovel.  At this stage, the vertex-ids are "pretranslated"
     * to a temporary internal ids. In the last phase, each vertex-id is assigned its
     * final id. 
     * 
     * AH46. THE MAIN REASON FOR PRETRANSLATION: The pretranslation is required because at this point we do not know
     * the total number of vertices.
     * @param part
     * @param preTranslatedIdFrom internal from-id
     * @param preTranslatedTo internal to-id
     * @param value -> the edge value
     * @throws IOException
     */
    private void addToShovel(int part, int src, int dest, EdgeValueType value) throws IOException {//ah46
    	
    	//The values being stored
    	System.out.print(String.valueOf(part)+" "+String.valueOf(src)+" "+String.valueOf(dest)+" ");
    	System.out.print(value);
    	System.out.println();
//    	System.exit(0);
//System.out.print(value);

        DataOutputStream strm = shovelStreams[part];
        strm.writeLong(packEdges(src, dest));
        if (edgeValueTypeBytesToValueConverter != null) {
            edgeValueTypeBytesToValueConverter.setValue(valueTemplate, value);
        }
        strm.write(valueTemplate);
        
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
     * Execute sharding by reading edges from a inputstream
     * @param inputStream
     * @param format graph input format
     * @throws IOException
     */
    public void pgen(InputStream inputStream, GraphInputFormat format) throws IOException {//ah46
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
    }

    /**
     * Partition a graph
     * @param inputStream
     * @param format "edgelist" or "adjlist" / "adjacency"
     * @throws IOException
     */
    public void pgen(InputStream inputStream, String format) throws IOException {//ah46
        if (format == null || format.equals("edgelist")) {
            pgen(inputStream, GraphInputFormat.EDGELIST);
        }
    }

    /**
     * Partition an input graph with edge list format.
     * @param inputStream
     * @throws IOException
     */
    public void pgen(InputStream inputStream) throws IOException {//ah46
        pgen(inputStream, GraphInputFormat.EDGELIST);
    }


}
