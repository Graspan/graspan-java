package edu.uci.ics.cs.gdtc.engine;

/**
 * This class consists of sorting methods.
 * 
 * @author Aftab
 *
 */
public class Optimizers {
	
	/**
	 * (Called by loadPartitions method()) Rendered from:
	 * http://www.programcreek.com/2012/11/quicksort-array-in-java/ accessed at
	 * 9 October 2015
	 * 
	 * For sorting each source vertex adjacency list in the loaded partitions
	 * 
	 * @param arr
	 * @param low
	 * @param high
	 */
	public static void quickSort(int[] edgeArr, byte[] edgeValArr, int low, int high) {
		if (edgeArr == null || edgeArr.length == 0)
			return;

		if (low >= high)
			return;

		// pick the pivot
		int middle = low + (high - low) / 2;
		int pivot = edgeArr[middle];

		// make left < pivot and right > pivot
		int i = low, j = high;
		while (i <= j) {
			while (edgeArr[i] < pivot) {
				i++;
			}

			while (edgeArr[j] > pivot) {
				j--;
			}

			if (i <= j) {

				int temp = edgeArr[i];
				edgeArr[i] = edgeArr[j];
				edgeArr[j] = temp;

				byte tempVal = edgeValArr[i];
				edgeValArr[i] = edgeValArr[j];
				edgeValArr[j] = tempVal;

				i++;
				j--;

			}
		}

		// recursively sort two sub parts
		if (low < j)
			quickSort(edgeArr, edgeValArr, low, j);

		if (high > i)
			quickSort(edgeArr, edgeValArr, i, high);
	}
	
}
