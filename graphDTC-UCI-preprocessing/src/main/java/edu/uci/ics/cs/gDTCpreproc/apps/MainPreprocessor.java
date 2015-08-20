package edu.uci.ics.cs.gDTCpreproc.apps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.gDTCpreproc.ChiFilenames;
import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.datablocks.FloatConverter;
import edu.uci.ics.cs.gDTCpreproc.io.CompressedIO;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.EdgeProcessor;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.PartitionGenerator;

/**
  * 
 * @author akyrola
 */
public class MainPreprocessor  {

	private static Logger logger = ChiLogger.getLogger("mainPreprocessor");

	/**
	 * Initialize the PartitionGenerator-program.
	 * 
	 * @param graphName
	 * @param numParts
	 * @return
	 * @throws IOException
	 */
	protected static PartitionGenerator createPartition(String graphName, int numParts) throws IOException {
		
		return new PartitionGenerator<Float, Float>(
				graphName, //ah46
				numParts, //ah46
				new EdgeProcessor<Float>() {//ah46
			public Float receiveEdge(int from, int to, String token) {
				return (token == null ? 0.0f : Float.parseFloat(token));
			}
		}, 
				new FloatConverter());//ah46
	}

	/**
	 * Usage: java edu.uci.ics.cs.gDTCpreproc.demo.PageRank graph-name num-shards
	 * filetype(edgelist|adjlist) For specifying the number of shards, 20-50
	 * million edges/shard is often a good configuration.
	 */
	public static void main(String[] args) throws Exception {
		String baseFilename = args[0];//ah46
		int nParts = Integer.parseInt(args[1]);//ah46
		String fileType = (args.length >= 3 ? args[2] : null);//ah46

		CompressedIO.disableCompression();//ah46

		/* Create partitions */
		
		/*
		 * ah46. creates the empty partitiongenerator data structure. Empty "shovel"
		 * #of each set = # input shards
		 */
		PartitionGenerator partgenerator = createPartition(baseFilename, nParts);
				
			/*
			 * ah46. checks whether the partition files already exist
			 */
			if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nParts)).exists()) {
				partgenerator.pgen(new FileInputStream(new File(baseFilename)), fileType);
			} else {
				logger.info("Found partitions -- no need to preprocess");
			}

	}
}
