package edu.uci.ics.cs.graspan.computationM;

import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.support.GraspanLogger;

public class SortedArrMerger {
	private static final Logger logger = GraspanLogger.getLogger("SortedArrMerger");

	private int delta_ptr = -1;

	private int oldUnewUdelta_ptr = -1;

	private int currentId;

//	private HashSet<Byte> currentEvals = new HashSet<Byte>();// TODO: Need to change this to byte array :DONE
	private byte[] currentEvals_arr;

	private int[] src_delta_edgs;
	private byte[] src_delta_vals;

	private int[] src_oldUnewUdelta_edgs;
	private byte[] src_oldUnewUdelta_vals;
	private PriorityQueue<MinSet> targetRowsMinHeap;

	public SortedArrMerger() {
		currentEvals_arr = new byte[GrammarChecker.getNumOfGrammarSymbols()];
		clearEvalsArr(currentEvals_arr);
//		logger.info("currentEvals_arr"+currentEvals_arr.length);
	}
	
	//*** new ds implementation methods ***
	public void clearEvalsArr(byte arr[]){
		for (int i = 0; i < arr.length; i++) {
			arr[i] = -1;
		}
	}
	
	public void addtoEvalsArr(byte arr[],byte eval){
		for (int i=0;i<arr.length;i++){
			if (currentEvals_arr[i]==-1){
				currentEvals_arr[i]=eval;
				break;
			}
		}
	}
	
	public boolean evalsArrContains(byte arr[], byte eval){
//		logger.info(arr.length+"LOOK HERE");
		for (int i=0;i<arr.length;i++){
			if (arr[i]==eval){
				return true;
			}
			if (arr[i]==-1){
				break;
			}
		}
		return false;
	}
	

	/**
	 * 
	 * @param edgArrstoMerge
	 * @param valArrstoMerge
	 * @param srcRowId
	 * @param delta_edgs
	 * @param delta_vals
	 * @param oldUnewUdelta_edgs
	 * @param oldUnewUdelta_vals
	 */
	public void mergeTgtstoSrc(int[][] edgArrstoMerge, byte[][] valArrstoMerge, int srcRowId) {
		assert (delta_ptr == -1);
		assert (srcRowId == 0);

		// MIN_SETS ARRAY
		MinSet[] minSets = new MinSet[edgArrstoMerge.length];

		targetRowsMinHeap = new PriorityQueue<MinSet>(edgArrstoMerge.length, new Comparator<MinSet>() {
			@Override
			public int compare(MinSet o1, MinSet o2) {
				return (o1.getCurrentVId() - o2.getCurrentVId());
			}
		});

		// INITIALIZE MIN SET FOR ROW i & track sizes to declare src_delta and
		// src_oldUnewUdelta
		int cumTgtRowsSize = 0;
		for (int i = 0; i < edgArrstoMerge.length; i++) {
			MinSet minSet = new MinSet(i);// TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. :DONE
			createNextMinSet(minSet, edgArrstoMerge[i], valArrstoMerge[i]);
			minSets[i] = minSet;

			// add each target row minSet to minHeap
			if (i != srcRowId)
				targetRowsMinHeap.offer(minSet);

			if (i != srcRowId)
				cumTgtRowsSize += edgArrstoMerge[i].length;
		}

		// declare & initialize src_delta and src_oldUnewUdelta
		src_delta_edgs = new int[cumTgtRowsSize];
		src_delta_vals = new byte[cumTgtRowsSize];
		// for (int i = 0; i < cumTgtRowsSize; i++) {
		// src_delta_edgs[i] = -1;
		// src_delta_vals[i] = -1;
		// }

		src_oldUnewUdelta_edgs = new int[edgArrstoMerge[srcRowId].length + cumTgtRowsSize];
		src_oldUnewUdelta_vals = new byte[edgArrstoMerge[srcRowId].length + cumTgtRowsSize];
		// for (int i = 0; i < edgArrstoMerge[srcRowId].length + cumTgtRowsSize;
		// i++) {
		// src_oldUnewUdelta_edgs[i] = -1;
		// src_oldUnewUdelta_vals[i] = -1;
		// }

		// MinSet minSetFrmTgtRows = null;
		// MinSet minSetFromSrcRow = null;

		if (edgArrstoMerge.length == 1) {
			processMinSetsForNull(edgArrstoMerge[srcRowId], valArrstoMerge[srcRowId]);
		} else {
			while (true) {
				// pick the min set from source row and min set from target rows
				MinSet minSetFromSrcRow = minSets[srcRowId];
				// MinSet minSetFrmTgtRows = getNextMinSetFrmTgtRows(minSets, srcRowId);
				MinSet minSetFrmTgtRows = targetRowsMinHeap.peek();
				if (minSetFromSrcRow.getCurrentVId() == Integer.MAX_VALUE && minSetFrmTgtRows.getCurrentVId() == Integer.MAX_VALUE) {
					break;
				}

				int rowIdOfTgtMinSet = minSetFrmTgtRows.getMinSetId();

				processMinSets(minSetFromSrcRow, minSetFrmTgtRows, edgArrstoMerge[srcRowId], valArrstoMerge[srcRowId],
						edgArrstoMerge[rowIdOfTgtMinSet], valArrstoMerge[rowIdOfTgtMinSet]); // TODO: NEED TO FIX.IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. :DONE

			}
		}

		// removing the empty values in output components: delta and oldUnewUdelta
		removeRedundantArraySpace(); // TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT.

	}

	/**
	 * Removes empty slots the arrays
	 */
	private void removeRedundantArraySpace() { // TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT.
		// removing empty values from src_oldUnewUdelta_edgs
		int[] tempEdgs = new int[oldUnewUdelta_ptr + 1];
		byte[] tempVals = new byte[oldUnewUdelta_ptr + 1];

		// for (int j = 0; j < oldUnewUdelta_ptr + 1; j++) {
		// tempEdgs[j] = src_oldUnewUdelta_edgs[j];
		// tempVals[j] = src_oldUnewUdelta_vals[j];
		// }
		System.arraycopy(src_oldUnewUdelta_edgs, 0, tempEdgs, 0, oldUnewUdelta_ptr + 1);
		System.arraycopy(src_oldUnewUdelta_vals, 0, tempVals, 0, oldUnewUdelta_ptr + 1);

		src_oldUnewUdelta_edgs = tempEdgs;
		src_oldUnewUdelta_vals = tempVals;

		// removing empty values from src_delta_edgs
		tempEdgs = new int[delta_ptr + 1];
		tempVals = new byte[delta_ptr + 1];

		// for (int j = 0; j < delta_ptr + 1; j++) {
		// tempEdgs[j] = src_delta_edgs[j];
		// tempVals[j] = src_delta_vals[j];
		// }
		System.arraycopy(src_delta_edgs, 0, tempEdgs, 0, delta_ptr + 1);
		System.arraycopy(src_delta_vals, 0, tempVals, 0, delta_ptr + 1);

		src_delta_edgs = tempEdgs;
		src_delta_vals = tempVals;
	}

	private void processMinSetsForNull(int[] srcEdgRow, byte[] srcValRow) {

		// add the source row minSet to src_oldUnewUdelta
		for (int i = 0; i < srcEdgRow.length; i++) {
			oldUnewUdelta_ptr++;
			if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
				src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = srcEdgRow[i];
				src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = srcValRow[i];
			} else {
				logger.info("Error, oldUnewUdelta_ptr has gone past the size the array!" + " ThreadNo:"
						+ Thread.currentThread().getId());
			}
		}

	}

	/**
	 * Generates the next minSet of a Row
	 * 
	 * @param minSet
	 *            - stores the minSet elements
	 * @param edgRow
	 *            - provides the vertex Id
	 * @param valRow
	 *            - provides the edge values
	 */
	public void createNextMinSet(MinSet minSet, int[] edgRow, byte[] valRow) {
		// logger.info("Updating MinSet no. " + minSet.getMinSetId() +
		// " ThreadNo:" + Thread.currentThread().getId());
		minSet.setCurrentVId(Integer.MAX_VALUE);
//		minSet.clearEvalSet(); //TODO: TO SWITCH METHOD :DONE
		minSet.clearEvalArr();
		for (int i = minSet.getPtr(); i < edgRow.length && edgRow[i] <= minSet.getCurrentVId(); i++) {
			minSet.setCurrentVId(edgRow[i]);
//			minSet.addEval(valRow[i]);//TODO: TO SWITCH METHOD :DONE
			minSet.addToEvalArr(valRow[i]);
			minSet.incrementPtr();
		}
	}

	/**
	 * get the minSet among all the target rows' minSets
	 * 
	 * @param minSets
	 * @param srcRowId
	 * @return
	 */
	public static MinSet getNextMinSetFrmTgtRows(MinSet[] minSets, int srcRowId) {
		int min = Integer.MAX_VALUE;
		MinSet minset = null;
		for (int i = 1; i < minSets.length; i++) {
			if (minSets[i].getCurrentVId() <= min) {
				// if (minSets[i].getCurrentVId() == -1) {
				// // if the minset has reached -1, ignore this minset
				// continue;
				// }
				min = minSets[i].getCurrentVId();
				minset = minSets[i];
			}
		}
		return minset;
	}

	public void processMinSets(MinSet minSetFrmSrcRow, MinSet minSetFrmTgtRows, int[] srcEdgRow, byte[] srcValRow,
			int[] tgtEdgRow, byte[] tgtValRow) {

		// case 1
		if (minSetFrmTgtRows.getCurrentVId() < minSetFrmSrcRow.getCurrentVId()) {
			if (currentId != minSetFrmTgtRows.getCurrentVId()) {
				currentId = minSetFrmTgtRows.getCurrentVId();
//				currentEvals.clear(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
				clearEvalsArr(currentEvals_arr);
				
			}

//			HashSet<Byte> evals_tgt = minSetFrmTgtRows.getEvals(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE 
			byte[] evals_tgt_arr = minSetFrmTgtRows.getEvalsArr();

//			for (Byte tgt_eval : evals_tgt) { // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
//				if (!currentEvals.contains(tgt_eval)) { // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			for (byte tgt_eval : evals_tgt_arr) {
				if (tgt_eval!=-1 & !this.evalsArrContains(currentEvals_arr, tgt_eval)){
					
					
					// add the target row minSet to src_oldUnewUdelta
					oldUnewUdelta_ptr++;
					if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
						src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = minSetFrmTgtRows.getCurrentVId();
						src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = tgt_eval;
					} else {
						logger.info("Error, oldUnewUdelta_ptr has gone past the size the array!" + " ThreadNo:" + Thread.currentThread().getId());
					}

					// add the target row minSet to delta
					delta_ptr++;
					if (delta_ptr < src_delta_edgs.length) {
						src_delta_edgs[delta_ptr] = minSetFrmTgtRows.getCurrentVId();
						src_delta_vals[delta_ptr] = tgt_eval;
//						currentEvals.add(tgt_eval); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
						this.addtoEvalsArr(currentEvals_arr, tgt_eval);
						
						
					} else {
						logger.info("Error, delta_ptr has gone past the size the array!" + " ThreadNo:"
								+ Thread.currentThread().getId());
					}
				}
			}
			// remove min from minheap
			targetRowsMinHeap.remove(minSetFrmTgtRows);

			// increment the pointers for this minset
			if (tgtEdgRow.length > 0) {
				createNextMinSet(minSetFrmTgtRows, tgtEdgRow, tgtValRow);
				// add current minset to minheap
				targetRowsMinHeap.offer(minSetFrmTgtRows);
			}
			return;

		}

		// case 2
		if (minSetFrmTgtRows.getCurrentVId() == minSetFrmSrcRow.getCurrentVId()) {
			if (currentId != minSetFrmTgtRows.getCurrentVId()) {
				currentId = minSetFrmTgtRows.getCurrentVId();
//				currentEvals.clear(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
				clearEvalsArr(currentEvals_arr);
			}
//			HashSet<Byte> evals_src, evals_tgt; // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			byte[] evals_src_arr, evals_tgt_arr;
			
			
//			evals_tgt = minSetFrmTgtRows.getEvals(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			evals_tgt_arr = minSetFrmTgtRows.getEvalsArr();
			
//			evals_src = minSetFrmSrcRow.getEvals(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			evals_src_arr = minSetFrmSrcRow.getEvalsArr();
			
//			for (Byte tgt_eval : evals_tgt) { // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			for (byte tgt_eval : evals_tgt_arr) {
				
				// compare the src evals and the tgt evals
//				if (!evals_src.contains(tgt_eval)) { // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
				if (tgt_eval!=-1 & !this.evalsArrContains(evals_src_arr, tgt_eval)){

//					if (!currentEvals.contains(tgt_eval)) { // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
					if (!this.evalsArrContains(currentEvals_arr, tgt_eval)){

						// add the target row minSet to src_oldUnewUdelta
						oldUnewUdelta_ptr++;
						if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
							src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = minSetFrmTgtRows.getCurrentVId();
							src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = tgt_eval;
						} else {
							logger.info("Error, oldUnewUdelta_ptr has gone past the size the array!" + " ThreadNo:"
									+ Thread.currentThread().getId());
						}

						// add the target row minSet to delta
						delta_ptr++;
						if (delta_ptr < src_delta_edgs.length) {
							src_delta_edgs[delta_ptr] = minSetFrmTgtRows.getCurrentVId();
							src_delta_vals[delta_ptr] = tgt_eval;
//							currentEvals.add(tgt_eval); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
							this.addtoEvalsArr(currentEvals_arr, tgt_eval);
						} else {
							logger.info("Error, delta_ptr has gone past the size the array!" + " ThreadNo:"
									+ Thread.currentThread().getId());
						}
					}
				}
			}
			// remove min from minheap
			targetRowsMinHeap.remove(minSetFrmTgtRows);

			// increment the pointers for this minset
			if (tgtEdgRow.length > 0) {
				createNextMinSet(minSetFrmTgtRows, tgtEdgRow, tgtValRow);
				// add current minset to minheap
				targetRowsMinHeap.offer(minSetFrmTgtRows);
			}
			return;
		}

		// case 3
		if (minSetFrmTgtRows.getCurrentVId() > minSetFrmSrcRow.getCurrentVId()) {

			if (currentId != minSetFrmSrcRow.getCurrentVId()) {
				currentId = minSetFrmSrcRow.getCurrentVId();
//				currentEvals.clear(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
				this.clearEvalsArr(currentEvals_arr);
			}

//			HashSet<Byte> evals_src; // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			byte[] evals_src_arr;

//			evals_src = minSetFrmSrcRow.getEvals(); // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
			evals_src_arr = minSetFrmSrcRow.getEvalsArr();

			// add the source row minSet to src_oldUnewUdelta
//			for (Byte src_eval : evals_src) { // TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. :DONE
			for (byte src_eval : evals_src_arr) {
//				if (!currentEvals.contains(src_eval)) { // TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
				if (src_eval!=-1 & !evalsArrContains(currentEvals_arr, src_eval)){
					oldUnewUdelta_ptr++;
					if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
						src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = minSetFrmSrcRow.getCurrentVId();
						src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = src_eval;
//						currentEvals.add(src_eval); // TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. :DONE
						this.addtoEvalsArr(currentEvals_arr, src_eval);
					} else {
						logger.info("Error, oldUnewUdelta_ptr has gone past the size the array!" + " ThreadNo:" + Thread.currentThread().getId());
					}
				}
			}

			// increment the pointers for this minset
			if (srcEdgRow.length > 0) {
				createNextMinSet(minSetFrmSrcRow, srcEdgRow, srcValRow);
			}
			return;
		}

	}

	public static boolean tgt_eval_exists(int a, int b) {
		if (a == b)
			return true;
		else
			return false;
	}

	public int get_num_new_edges() {
		return delta_ptr + 1;
	}

	public int[] get_src_delta_edgs() {
		return src_delta_edgs;
	}

	public byte[] get_src_delta_vals() {
		return src_delta_vals;
	}

	public int[] get_src_oldUnewUdelta_edgs() {
		return src_oldUnewUdelta_edgs;
	}

	public byte[] get_src_oldUnewUdelta_vals() {
		return src_oldUnewUdelta_vals;
	}

	public static void main(String args[]) {

		// // GIVEN
		// int[][] new_edgs = new int[5][];
		// int[][] new_vals = new int[5][];
		//
		// new_edgs[0] = new int[] { 100, 100, 200, 200, 300, 1700, 1700 };
		// new_vals[0] = new int[] { 7, 2, 1, 3, 4, 6, 8 };
		//
		// new_edgs[1] = new int[] { 2, 17, 17, 17, 25 };
		// new_vals[1] = new int[] { 3, 4, 8, 6, 1 };
		//
		// new_edgs[2] = new int[] { 1, 3, 3, 6, 10 };
		// new_vals[2] = new int[] { 2, 5, 1, 17, 18 };
		//
		// new_edgs[3] = new int[] { 11, 12, 13, 14, 15, 15 };
		// new_vals[3] = new int[] { 10, 1, 2, 3, 4, 5 };
		//
		// new_edgs[4] = new int[] { 1, 4, 11, 21 };
		// new_vals[4] = new int[] { 6, 5, 10, 1 };
		//
		// // ALLOCATE DELTA
		// int[][] delta_edgs = new int[5][];
		// int[][] delta_vals = new int[5][];
		// int[][] oldUnewUdelta_edgs = new int[5][];
		// int[][] oldUnewUdelta_vals = new int[5][];
		//
		// // NO. OF ROWS IN ARRAY SET TO MERGE
		// int num = 3;
		//
		// int[][] edgArrstoMerge = new int[num][];
		// int[][] valArrstoMerge = new int[num][];
		//
		// // THE ROWS TO MERGE
		// edgArrstoMerge[0] = new_edgs[0];
		// valArrstoMerge[0] = new_vals[0];
		//
		// edgArrstoMerge[1] = new_edgs[1];
		// valArrstoMerge[1] = new_vals[1];
		//
		// edgArrstoMerge[2] = new_edgs[2];
		// valArrstoMerge[2] = new_vals[2];
		//
		// // ID OF SOURCE ROW (IN edgArrstoMerge)
		// int srcRowId = 0;
		//
		// logger.info("Source Array:");
		// logger.info(Arrays.toString(edgArrstoMerge[0]));
		// logger.info("Arrays to merge:");
		// logger.info(Arrays.deepToString(edgArrstoMerge));
		//
		// mergeTgtstoSrc(edgArrstoMerge, valArrstoMerge, srcRowId,
		// delta_edgs[srcRowId], delta_vals[srcRowId],
		// oldUnewUdelta_edgs[srcRowId], oldUnewUdelta_vals[srcRowId]);

		// logger.info("delta:");
		// logger.info(Arrays.toString(delta_edgs[srcRowId]));
		// logger.info(Arrays.deepToString(delta_vals));
		//
		// logger.info("oldUnewUdelta:");
		// logger.info(Arrays.deepToString(oldUnewUdelta_edgs));
		// logger.info(Arrays.deepToString(oldUnewUdelta_vals));
		// logger.info("delta:");
		// logger.info(Arrays.toString(src_delta_edgs));
		// logger.info(Arrays.toString(src_delta_vals));

	}
}
