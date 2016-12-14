package edu.uci.ics.cs.graspan.edgecomputationMerge;

import java.util.Arrays;

public class ArrayFunction {

	public static void main(String args[]) {
		int[] arr = new int[5];
		function(arr);
		System.out.println(Arrays.toString(arr));
		Class2 cls2 = new Class2();
		cls2.abc();
		System.out.println(Arrays.toString(cls2.get_a()));
	}

	public static void function(int arr[]) {
		arr[0] = 46;
	}

}
