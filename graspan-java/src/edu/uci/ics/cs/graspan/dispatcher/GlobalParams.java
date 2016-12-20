package edu.uci.ics.cs.graspan.dispatcher;

/**
 * 
 * @author Aftab 1 November 2015
 */
public class GlobalParams {

	// input graph full file name and path
	public static String baseFilename = "";
	
	// total number of partitions
	static int numParts;
	
	// in-memory computation
	public static boolean inMemComp;

	// number of partitions during each computation
	static final int NUM_PARTS_PER_COMPUTATION = 2;

	// The strategy for reloading partitions;
	// RELOAD_PLAN_1 - Reload all the requested partitions everytime,
	// regardless of which are already in the memory
	// RELOAD_PLAN_2 - Reload only the requested partitions that are not in
	// the memory. If a partition has been repartitioned, we consider it not to
	// be in the memory.
	// RELOAD_PLAN_3 - Reload only the requested partitions that are not in
	// the memory, however, if a partition has been repartitioned and a
	// requested partition is one of its child partitions, we keep the child
	// partition
	static String reloadPlan = "";

	static String computation_logic = "";
	
	private static final int GRAMMAR_SIZE = 200;

	// The grammar file
	private static byte[][] grammarTab = new byte[GRAMMAR_SIZE][3];

	// The size of the Edge Destination Count Table
	private static int EdcSize;

	// Output edge tracker interval
	private static int OpEdgeTrackerInterval;

	// Size of each new edges node
	private static int NewEdgeNodeSize;

	// Maximum size of a partition after adding new edges
	private static long PartMaxPostNewEdges;

	// The type of input graph: DATAFLOW OR POINTS-TO (used to indicate whether
	// or not edge values exist)
	private static String hasEdgeVals;

	// The id of the first vertex of the graph
	private static int firstVId;
	
	// The number of threads
	private static int num_Threads;
	
	// The type of preprocessing operation: +eRULE or GenParts 
	private static String ppOperation;
	
	// DATAFLOW or POINTSTO: if dataflow, then erules adding process is skipped
	private static String analysisType;

	private static int heapSize;
	
	private static double pSizeConst, repartConst;

	public static void setEdcSize(int num) {
		EdcSize = num;
	}
	
	public static int getEdcSize() {
		return EdcSize;
	}
	
	public static boolean inMemComp(){
		return inMemComp;
	}
	
	public static void setInMemComp(boolean val){
		inMemComp=val;
	}
	
	
	public static void setOpEdgeTrackerInterval(int num) {
		OpEdgeTrackerInterval = num;
	}

	public static int getOpEdgeTrackerInterval() {
		return OpEdgeTrackerInterval;
	}

	public static void setNewEdgesNodeSize(int num) {
		NewEdgeNodeSize = num;
	}

	public static int getNewEdgesNodeSize() {
		return NewEdgeNodeSize;
	}

	public static void setPartMaxPostNewEdges(long num) {
		PartMaxPostNewEdges = num;
	}

	public static long getPartMaxPostNewEdges() {
		return PartMaxPostNewEdges;
	}

	/**
	 * 
	 * @return String baseFilename
	 */
	public static String getBasefilename() {
		return baseFilename;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setBasefilename(String str) {
		baseFilename = str;
	}

	/**
	 * 
	 * @return int numParts
	 */
	public static int getNumParts() {
		return numParts;
	}

	/**
	 * 
	 * @param n
	 */
	public static void setNumParts(int n) {
		numParts = n;
	}

	/**
	 * 
	 * @return int numPartsPerComputation
	 */
	public static int getNumPartsPerComputation() {
		return NUM_PARTS_PER_COMPUTATION;
	}

	/**
	 * 
	 * @return
	 */
	public static String getReloadPlan() {
		return reloadPlan;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setReloadPlan(String str) {
		reloadPlan = str;
	}

	/**
	 * 
	 * @return
	 */
	public static byte[][] getGrammarTab() {
		return grammarTab;
	}

	/**
	 * 
	 * @param str
	 */
	public static void setGrammarTab(byte[][] arr) {
		grammarTab = arr;
	}

	public static String hasEdgeVals() {
		return hasEdgeVals;
	}
	

	public static void setHasEdgeVals(String hasedgvals) {
		hasEdgeVals = hasedgvals;

	}
	
	public static int getFirstVertexID() {
		return firstVId;
	}

	public static void setFirstVertexID(int id) {
		firstVId = id;
	}

	public static void setComputationLogic(String compLogic) {
		computation_logic = compLogic;
	}

	public static String getComputationLogic() {
		return computation_logic;
	}

	public static void setNumThreads(int numThreads) {
		num_Threads=numThreads;
	}
	
	public static int getNumThreads() {
		return num_Threads;
	}

	public static void setPPOperation(String ppOP) {
		ppOperation = ppOP;
	}
	
	public static String getPPOperation(){
		return ppOperation;
	}

	public static void setHeapSize(int hs) {
		heapSize=hs;
	}
	
	public static int getHeapSize(){
		return heapSize;
	}
	
	public static void setRepartConst(double rc){
		repartConst=rc;
	}
	
	public static double getRepartConst(){
		return repartConst;
	}
	
	public static void setPSizeConst(double pc){
		pSizeConst=pc;
	}
	
	public static double getPSizeConst(){
		return pSizeConst;
	}

}
