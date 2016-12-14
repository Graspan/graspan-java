package edu.uci.ics.cs.graspan.edgecomputationMerge;

import java.util.Arrays;

public class ArrayCopy {

	public static void main(String args[]) {
		int[] arr = new int[] { 1, 2, 3, 4, 5 };
		int[] arr2 = new int[2];
		arr2[0] = arr[0];
		arr2[1] = arr[1];
		System.out.println("arr2 " + Arrays.toString(arr2));
		// arr[1] = 46;
		arr = arr2;
		System.out.println("arr " + Arrays.toString(arr));
	}

}
