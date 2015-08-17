package edu.uci.ics.cs.gDTCpreproc.toolkits.collaborative_filtering;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

import edu.uci.ics.cs.gDTCpreproc.ChiFilenames;
import edu.uci.ics.cs.gDTCpreproc.ChiLogger;
import edu.uci.ics.cs.gDTCpreproc.datablocks.FloatConverter;
import edu.uci.ics.cs.gDTCpreproc.datablocks.IntConverter;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.EdgeProcessor;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.FastSharder;
import edu.uci.ics.cs.gDTCpreproc.util.HugeDoubleMatrix;

public class ProblemSetup {

        static HugeDoubleMatrix latent_factors_inmem;
		static long M,N,L;
        static int D = 10;
        static double minval = -1e100;
        static double maxval = 1e100;
        protected static Logger logger = ChiLogger.getLogger("ALS");
        static double train_rmse = 0.0;
        FastSharder sharder_validation;
        static RMSEEngine validation_rmse_engine;
        static String training;
        static String validation;
        static String test;
        static int nShards;
        static int quiet;
        
        void init_feature_vectors(long size){
          logger.info("Initializing latent factors for " + size + " vertices");
          latent_factors_inmem = new HugeDoubleMatrix(size, D);

          /* Fill with random data */
          latent_factors_inmem.randomize(0f, 1.0f);
        }
        
       
      
}
