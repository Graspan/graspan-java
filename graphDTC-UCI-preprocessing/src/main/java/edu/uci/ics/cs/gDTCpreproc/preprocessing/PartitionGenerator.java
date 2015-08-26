package edu.uci.ics.cs.gDTCpreproc.preprocessing;

import java.io.*;
import java.util.logging.Logger;

import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.datablocks.GenericIntegerConverter;

public class PartitionGenerator<VertexValueType, EdgeValueType> {

	public enum GraphInputFormat {
		EDGELIST, ADJACENCY, MATRIXMARKET
	};

	private String baseFilename;
	private int nParts;

	private DataOutputStream[] shovelStreams;


	private GenericIntegerConverter vIdtoBytes;
	private GenericIntegerConverter edgeValtoBytes;
	

	private static final Logger logger = ChiLogger.getLogger("fast-sharder");

	/**
	 * Constructor
	 * 
	 * @param baseFilename input-file
	 * @param nParts the number of partitions to be created
	 * @param edgeValConverter translator byte-arrays to/from edge-value
	 * @throws IOException if problems reading the data
	 */
	public PartitionGenerator(String baseFilename, int nParts, GenericIntegerConverter vIdtoBytes, 
			GenericIntegerConverter edgeValConverter) throws IOException {
		this.baseFilename = baseFilename;
		this.nParts = nParts;
		this.vIdtoBytes = vIdtoBytes;
		this.edgeValtoBytes = edgeValConverter; 

		/**
		 * the edges are "shoveled" to partitions.
		 */
		shovelStreams = new DataOutputStream[nParts];
		for (int i = 0; i < nParts; i++) {

			/*
			 * ah46. Empty "shovel" files are created"
			 */
			shovelStreams[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(shovelFilename(i))));
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
	 * 
	 * @param src
	 * @param dest
	 * @param edgeValue
	 * @throws IOException
	 */
	public void addEdge(int src, int dest, int edgeValue) throws IOException {// ah46

		/*
		 * TODO here edges are grouped by dest vertex id % number of partitions
		 * we need to change this so that edges with the same group of source
		 * vertices are in the same partition
		 */
		addToShovel(dest % nParts, src, dest, edgeValue);// ah46
	}

	private byte[] valueTemplate;
	
	private void addToShovel(int part, int src, int dest, int edgeValue) throws IOException {// ah46

		// The values being stored
		// System.out.print(String.valueOf(part)+" "+String.valueOf(src)+"
		// "+String.valueOf(dest)+" ");
		// System.out.print(edgeValue);
		// System.out.println();
		// System.exit(0);
		// System.out.print(()value);
		
		DataOutputStream strm = shovelStreams[part];
		
		valueTemplate=new byte[vIdtoBytes.getSize()];
		
		vIdtoBytes.setValue(valueTemplate, src);
		strm.write(valueTemplate);
		
		vIdtoBytes.setValue(valueTemplate, dest);
		strm.write(valueTemplate);
		
		valueTemplate=new byte[edgeValtoBytes.getSize()];
		
		edgeValtoBytes.setValue(valueTemplate, edgeValue);
		strm.write(valueTemplate);
		
//		edgeValtoBytes.setValue(valueTemplate, edgeValue);
//		strm.writeLong(packEdges(src, dest));
//		if (edgeValtoBytes != null) {
//			edgeValtoBytes.setValue(valueTemplate, edgeValue);
//		}

	}


	/**
	 * generates the partitions
	 * 
	 * @param inputStream
	 * @param format graph input format
	 * @throws IOException
	 */
	public void pgen(InputStream inputStream) throws IOException {
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
		long partMax=edgeCounter(inputStream)/nParts;
		String ln;
		long lineNum = 0;
		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				lineNum++;
				if (lineNum % 100000 == 0)
					logger.info("Reading line: " + lineNum);
				String[] tok = ln.split("\t");

				/* Edge list: <src> <dst> <value> */
				this.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]), Integer.parseInt(tok[2]));
			}
		}
	}
	
	/**
	 * Scans the entire graph and counts the total number of edges. 
	 * 
	 * @param inputStream
	 * @param format
	 * @throws IOException
	 */
	public long edgeCounter(InputStream inputStream) throws IOException {// ah46
		BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));// ah46
		String ln;
		long numEdges = 0;
		while ((ln = ins.readLine()) != null) {
			if (!ln.startsWith("#")) {
				numEdges++;
				if (numEdges % 100000 == 0)
					logger.info("Reading line: " + numEdges);
			}
		}
		return numEdges;
	}


}
