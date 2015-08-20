package edu.uci.ics.cs.gDTCpreproc.apps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.gDTCpreproc.ChiFilenames;
import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.ChiVertex;
import edu.uci.ics.cs.gDTCpreproc.GraphChiContext;
import edu.uci.ics.cs.gDTCpreproc.GraphChiProgram;
import edu.uci.ics.cs.gDTCpreproc.datablocks.FloatConverter;
import edu.uci.ics.cs.gDTCpreproc.engine.VertexInterval;
import edu.uci.ics.cs.gDTCpreproc.io.CompressedIO;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.EdgeProcessor;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.FastSharder;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.VertexProcessor;

/**
 * Example application: PageRank (http://en.wikipedia.org/wiki/Pagerank)
 * Iteratively computes a pagerank for each vertex by averaging the pageranks of
 * in-neighbors pageranks.
 * 
 * @author akyrola
 */
public class Pagerank implements GraphChiProgram<Float, Float> {

	private static Logger logger = ChiLogger.getLogger("pagerank");

	public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {
		if (context.getIteration() == 0) {
			/* Initialize on first iteration */
			vertex.setValue(1.0f);
		} else {
			/*
			 * On other iterations, set my value to be the weighted average of
			 * my in-coming neighbors pageranks.
			 */
			float sum = 0.f;
			for (int i = 0; i < vertex.numInEdges(); i++) {
				sum += vertex.inEdge(i).getValue();
			}
			vertex.setValue(0.15f + 0.85f * sum);
		}

		/*
		 * Write my value (divided by my out-degree) to my out-edges so
		 * neighbors can read it.
		 */
		float outValue = vertex.getValue() / vertex.numOutEdges();
		for (int i = 0; i < vertex.numOutEdges(); i++) {
			vertex.outEdge(i).setValue(outValue);
		}

	}

	/**
	 * Callbacks (not needed for Pagerank)
	 */
	public void beginIteration(GraphChiContext ctx) {
	}

	public void endIteration(GraphChiContext ctx) {
	}

	public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
	}

	public void endInterval(GraphChiContext ctx, VertexInterval interval) {
	}

	public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
	}

	public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
	}

	/**
	 * Initialize the sharder-program.
	 * 
	 * @param graphName
	 * @param numShards
	 * @return
	 * @throws IOException
	 */
	protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
		
		return new FastSharder<Float, Float>(
				graphName, //ah46
				numShards, //ah46
				new VertexProcessor<Float>() {//ah46
			public Float receiveVertexValue(int vertexId, String token) {
				return (token == null ? 0.0f : Float.parseFloat(token));
			}
		}, 
				new EdgeProcessor<Float>() {//ah46
			public Float receiveEdge(int from, int to, String token) {
				return (token == null ? 0.0f : Float.parseFloat(token));
			}
		}, 
				new FloatConverter(), //ah46
				new FloatConverter());//ah46
		
		
	}

	/**
	 * Usage: java edu.uci.ics.cs.gDTCpreproc.demo.PageRank graph-name num-shards
	 * filetype(edgelist|adjlist) For specifying the number of shards, 20-50
	 * million edges/shard is often a good configuration.
	 */
	public static void main(String[] args) throws Exception {
		String baseFilename = args[0];//ah46
		int nShards = Integer.parseInt(args[1]);//ah46
		String fileType = (args.length >= 3 ? args[2] : null);//ah46

		CompressedIO.disableCompression();//ah46

		/* Create shards */
		
		/*
		 * ah46. creates the empty sharder data structure. Empty "shovel" and "vertexshovel files are created"
		 * #of each set = # input shards
		 */
		FastSharder sharder = createSharder(baseFilename, nShards);
				
			/*
			 * ah46. checks whether the shard files already exist
			 */
			if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
				sharder.shard(new FileInputStream(new File(baseFilename)), fileType);
			} else {
				logger.info("Found shards -- no need to preprocess");
			}

		/* Run GraphChi */
		// GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float,
		// Float>(baseFilename, nShards);
		// engine.setEdataConverter(new FloatConverter());
		// engine.setVertexDataConverter(new FloatConverter());
		// engine.setModifiesInedges(false); // Important optimization
		//
		// engine.run(new Pagerank(), 4);
		//
		// logger.info("Ready.");
		//
		// /* Output results */
		// int i = 0;
		// VertexIdTranslate trans = engine.getVertexIdTranslate();
		// TreeSet<IdFloat> top20 = Toplist.topListFloat(baseFilename,
		// engine.numVertices(), 20);
		// for(IdFloat vertexRank : top20) {
		// System.out.println(++i + ": " +
		// trans.backward(vertexRank.getVertexId()) + " = " +
		// vertexRank.getValue());
		// }
	}
}
