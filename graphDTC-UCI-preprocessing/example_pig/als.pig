
REGISTER graphchi-java-0.2-jar-with-dependencies.jar;

factors = LOAD '/user/akyrola/graphs/netflix_ratings' USING edu.uci.ics.cs.gDTCpreproc.apps.pig.PigALSMatrixFactorization;

STORE factors INTO '/user/akyrola/als_factors';

