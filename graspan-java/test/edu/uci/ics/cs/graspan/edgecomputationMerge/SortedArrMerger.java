package edu.uci.ics.cs.graspan.edgecomputationMerge;

import java.util.Arrays;
import java.util.HashSet;

public class SortedArrMerger {

	static int delta_ptr = -1;
	static int oldUnewUdelta_ptr = -1;
	static int currentId;
	static HashSet<Integer> currentEvals = new HashSet<Integer>();

	public static void main(String args[]) {

		// GIVEN
		int[][] new_edgs = new int[5][];
		int[][] new_vals = new int[5][];

		new_edgs[0] = new int[] { 100, 100, 200, 200, 300, 1700, 1700 };
		new_vals[0] = new int[] { 7, 2, 1, 3, 4, 6, 8 };

		new_edgs[1] = new int[] { 2, 17, 17, 17, 25 };
		new_vals[1] = new int[] { 3, 4, 8, 6, 1 };

		new_edgs[2] = new int[] { 1, 3, 3, 6, 10 };
		new_vals[2] = new int[] { 2, 5, 1, 17, 18 };

		new_edgs[3] = new int[] { 11, 12, 13, 14, 15, 15 };
		new_vals[3] = new int[] { 10, 1, 2, 3, 4, 5 };

		new_edgs[4] = new int[] { 1, 4, 11, 21 };
		new_vals[4] = new int[] { 6, 5, 10, 1 };

		// ALLOCATE DELTA
		int[][] delta_edgs = new int[5][];
		int[][] delta_vals = new int[5][];
		int[][] oldUnewUdelta_edgs = new int[5][];
		int[][] oldUnewUdelta_vals = new int[5][];

		// NO. OF ROWS IN ARRAY SET TO MERGE
		int num = 3;

		int[][] edgArrstoMerge = new int[num][];
		int[][] valArrstoMerge = new int[num][];

		// THE ROWS TO MERGE
		edgArrstoMerge[0] = new_edgs[0];
		valArrstoMerge[0] = new_vals[0];

		edgArrstoMerge[1] = new_edgs[1];
		valArrstoMerge[1] = new_vals[1];

		edgArrstoMerge[2] = new_edgs[2];
		valArrstoMerge[2] = new_vals[2];

		// ID OF SOURCE ROW (IN edgArrstoMerge)
		int srcRowId = 0;

		System.out.println("Source Array:");
		System.out.println(Arrays.toString(edgArrstoMerge[0]));
		System.out.println("Arrays to merge:");
		System.out.println(Arrays.deepToString(edgArrstoMerge));

		mergeTgtstoSrc(edgArrstoMerge, valArrstoMerge, srcRowId,
				delta_edgs[srcRowId], delta_vals[srcRowId],
				oldUnewUdelta_edgs[srcRowId], oldUnewUdelta_vals[srcRowId]);

		// System.out.println("delta:");
		// System.out.println(Arrays.toString(delta_edgs[srcRowId]));
		// System.out.println(Arrays.deepToString(delta_vals));
		//
		// System.out.println("oldUnewUdelta:");
		// System.out.println(Arrays.deepToString(oldUnewUdelta_edgs));
		// System.out.println(Arrays.deepToString(oldUnewUdelta_vals));
		// System.out.println("delta:");
		// System.out.println(Arrays.toString(src_delta_edgs));
		// System.out.println(Arrays.toString(src_delta_vals));

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
	public static void mergeTgtstoSrc(int[][] edgArrstoMerge,
			int[][] valArrstoMerge, int srcRowId, int[] src_delta_edgs,
			int[] src_delta_vals, int[] src_oldUnewUdelta_edgs,
			int[] src_oldUnewUdelta_vals) {

		MinSet minSetFrmTgtRows = new MinSet();
		MinSet minSetFromSrcRow = new MinSet();

		// MIN_SETS ARRAY
		MinSet[] minSets = new MinSet[edgArrstoMerge.length];

		// INITIALIZE MIN SET FOR ROW i & track sizes to declare src_delta and
		// src_oldUnewUdelta
		int cumTgtRowsSize = 0;
		for (int i = 0; i < edgArrstoMerge.length; i++) {
			minSets[i] = new MinSet();
			minSets[i].setMinSetId(i);
			if (i != srcRowId)
				cumTgtRowsSize += edgArrstoMerge[i].length;
		}

		// declare & initialize src_delta and src_oldUnewUdelta

		src_delta_edgs = new int[cumTgtRowsSize];
		src_delta_vals = new int[cumTgtRowsSize];
		for (int i = 0; i < cumTgtRowsSize; i++) {
			src_delta_edgs[i] = -1;
			src_delta_vals[i] = -1;
		}

		src_oldUnewUdelta_edgs = new int[edgArrstoMerge[srcRowId].length
				+ cumTgtRowsSize];
		src_oldUnewUdelta_vals = new int[edgArrstoMerge[srcRowId].length
				+ cumTgtRowsSize];
		for (int i = 0; i < edgArrstoMerge[srcRowId].length + cumTgtRowsSize; i++) {
			src_oldUnewUdelta_edgs[i] = -1;
			src_oldUnewUdelta_vals[i] = -1;
		}

		// generate the minSets for all rows to merge
		int i = 0;
		for (MinSet minSet : minSets) {
			if (edgArrstoMerge[i].length > 0) {
				if (minSet.getPtr() == -1) {
					minSet.incrementPtr();
				}
				createNextMinSet(minSet, edgArrstoMerge[i], valArrstoMerge[i]);
			}
			i++;
		}

		// MinSet Test
//		System.out.println("Minsets:");
//		for (MinSet minSet : minSets) {
//			System.out.println(minSet);
//		}

		System.out.println("Starting processing of Minsets");

		while (true) {
			// pick the min set from source row and min set from target rows
			minSetFromSrcRow = minSets[srcRowId];
			minSetFrmTgtRows = getNextMinSetFrmTgtRows(minSets, srcRowId);
			if (minSetFromSrcRow.getCurrentVId() == Integer.MAX_VALUE
					&& minSetFrmTgtRows.getCurrentVId() == Integer.MAX_VALUE) {
				break;
			}
			System.out.println("minSetFrmTgtRows: " + minSetFrmTgtRows);
			int rowIdOfTgtMinSet = minSetFrmTgtRows.getMinSetId();

			processMinSets(minSetFromSrcRow, minSetFrmTgtRows, src_delta_edgs,
					src_delta_vals, src_oldUnewUdelta_edgs,
					src_oldUnewUdelta_vals, edgArrstoMerge[srcRowId],
					valArrstoMerge[srcRowId], edgArrstoMerge[rowIdOfTgtMinSet],
					valArrstoMerge[rowIdOfTgtMinSet]);

			// MinSet Test
//			System.out.println("MinSets");
//			for (MinSet minSet : minSets) {
//				System.out.println(minSet);
//			}

//			System.out.println("delta:");
//			System.out.println(Arrays.toString(src_delta_edgs));
//			System.out.println(Arrays.toString(src_delta_vals));
//
//			System.out.println("oldUnewUdelta:");
//			System.out.println(Arrays.toString(src_oldUnewUdelta_edgs));
//			System.out.println(Arrays.toString(src_oldUnewUdelta_vals));
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
	public static void createNextMinSet(MinSet minSet, int[] edgRow,
			int[] valRow) {
		System.out.println("Updating MinSet no. " + minSet.getMinSetId());
		minSet.setCurrentVId(Integer.MAX_VALUE);
		minSet.clearEvalSet();
		for (int i = minSet.getPtr(); i < edgRow.length; i++) {
			if (edgRow[i] <= minSet.getCurrentVId()) {
				minSet.setCurrentVId(edgRow[i]);
				minSet.addEval(valRow[i]);
				minSet.incrementPtr();
			} else
				break;
		}
	}

	public static MinSet getNextMinSetFrmTgtRows(MinSet[] minSets, int srcRowId) {
		int min = Integer.MAX_VALUE;
		MinSet minset = null;
		for (int i = 0; i < minSets.length; i++) {
			if (i != srcRowId) {
				if (minSets[i].getCurrentVId() <= min) {
					min = minSets[i].getCurrentVId();
					minset = minSets[i];
				}
			}
		}
		return minset;
	}

	public static void processMinSets(MinSet minSetFrmSrcRow,
			MinSet minSetFrmTgtRows, int[] src_delta_edgs,
			int[] src_delta_vals, int[] src_oldUnewUdelta_edgs,
			int[] src_oldUnewUdelta_vals, int[] srcEdgRow, int[] srcValRow,
			int[] tgtEdgRow, int[] tgtValRow) {

		System.out.println("Processing SrcMinSet # "
				+ minSetFrmSrcRow.getMinSetId() + " and TgtMinSet # "
				+ minSetFrmTgtRows.getMinSetId());

		// System.out.println("minset from target rows:");
		// System.out.println(minSetFrmTgtRows.getCurrentVId());
		// System.out.println(minSetFrmTgtRows.getEvals());
		// System.out.println("minset from source row:");
		// System.out.println(minSetFrmSrcRow.getCurrentVId());
		// System.out.println(minSetFrmSrcRow.getEvals());

		// case 1
		if (minSetFrmTgtRows.getCurrentVId() < minSetFrmSrcRow.getCurrentVId()) {
			if (currentId != minSetFrmTgtRows.getCurrentVId()) {
				currentId = minSetFrmTgtRows.getCurrentVId();
				currentEvals.clear();
			}

			HashSet<Integer> evals_tgt;
			evals_tgt = minSetFrmTgtRows.getEvals();
			for (Integer tgt_eval : evals_tgt) {
				if (!currentEvals.contains(tgt_eval)) {

					// add the target row minSet to src_oldUnewUdelta
					oldUnewUdelta_ptr++;
					if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
						src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = minSetFrmTgtRows
								.getCurrentVId();
						src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = tgt_eval;
					} else {
						System.out
								.println("Error, oldUnewUdelta_ptr has gone past the size the array!");
					}

					// add the target row minSet to delta
					delta_ptr++;
					if (delta_ptr < src_delta_edgs.length) {
						src_delta_edgs[delta_ptr] = minSetFrmTgtRows
								.getCurrentVId();
						src_delta_vals[delta_ptr] = tgt_eval;
						currentEvals.add(tgt_eval);
					} else {
						System.out
								.println("Error, delta_ptr has gone past the size the array!");
					}
				}
			}
			// increment the pointers for this minset
			if (tgtEdgRow.length > 0) {
				createNextMinSet(minSetFrmTgtRows, tgtEdgRow, tgtValRow);
			}
			return;

		}

		// case 2
		if (minSetFrmTgtRows.getCurrentVId() == minSetFrmSrcRow.getCurrentVId()) {
			if (currentId != minSetFrmTgtRows.getCurrentVId()) {
				currentId = minSetFrmTgtRows.getCurrentVId();
				currentEvals.clear();
			}
			HashSet<Integer> evals_src, evals_tgt;
			evals_tgt = minSetFrmTgtRows.getEvals();
			evals_src = minSetFrmSrcRow.getEvals();

			for (Integer tgt_eval : evals_tgt) {
				// compare the src evals and the tgt evals
				if (!evals_src.contains(tgt_eval)) {

					if (!currentEvals.contains(tgt_eval)) {

						// add the target row minSet to src_oldUnewUdelta
						oldUnewUdelta_ptr++;
						if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
							src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = minSetFrmTgtRows
									.getCurrentVId();
							src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = tgt_eval;
						} else {
							System.out
									.println("Error, oldUnewUdelta_ptr has gone past the size the array!");
						}

						// add the target row minSet to delta
						delta_ptr++;
						if (delta_ptr < src_delta_edgs.length) {
							src_delta_edgs[delta_ptr] = minSetFrmTgtRows
									.getCurrentVId();
							src_delta_vals[delta_ptr] = tgt_eval;
							currentEvals.add(tgt_eval);
						} else {
							System.out
									.println("Error, delta_ptr has gone past the size the array!");
						}
					}
				}
			}

			// increment the pointers for this minset
			createNextMinSet(minSetFrmTgtRows, tgtEdgRow, tgtValRow);
			return;
		}

		// case 3
		if (minSetFrmTgtRows.getCurrentVId() > minSetFrmSrcRow.getCurrentVId()) {

			if (currentId != minSetFrmSrcRow.getCurrentVId()) {
				currentId = minSetFrmSrcRow.getCurrentVId();
				currentEvals.clear();
			}

			HashSet<Integer> evals_src, evals_tgt;

			evals_tgt = minSetFrmTgtRows.getEvals();
			evals_src = minSetFrmSrcRow.getEvals();

			// add the source row minSet to src_oldUnewUdelta
			for (Integer src_eval : evals_src) {
				if (!currentEvals.contains(src_eval)) {

					oldUnewUdelta_ptr++;
					if (oldUnewUdelta_ptr < src_oldUnewUdelta_edgs.length) {
						src_oldUnewUdelta_edgs[oldUnewUdelta_ptr] = minSetFrmSrcRow
								.getCurrentVId();
						src_oldUnewUdelta_vals[oldUnewUdelta_ptr] = src_eval;
						currentEvals.add(src_eval);
					} else {
						System.out
								.println("Error, oldUnewUdelta_ptr has gone past the size the array!");
					}
				}
			}

			// increment the pointers for this minset
			createNextMinSet(minSetFrmSrcRow, srcEdgRow, srcValRow);
			return;
		}

	}

	public static boolean tgt_eval_exists(int a, int b) {
		if (a == b)
			return true;
		else
			return false;
	}

	public static void trial(int[] arr) {
		arr = new int[5];
	}

}
