package edu.uci.ics.cs.gdtc.edgecomputation;

public class InitializeTwoDimArrTest {

	public static void main(String args[]) {

		int[][] arr = new int[5][];
		int degs[] = { 5,3,2,1,10};
		initialize2DPartArray(arr, 5, degs);
		
		for (int i=0;i<5;i++){
			for (int j=0;j<degs[i];j++){
				System.out.print(arr[i][j]);
			}
			System.out.println();
		}
		
		System.out.println(arr.length+"which length");
	}

	private static void initialize2DPartArray(int[][] partArray, int numOfUniqueSrcs, int[] partDegs) {
		for (int i = 0; i < numOfUniqueSrcs; i++) {
			partArray[i] = new int[partDegs[i]];
		}
		for (int i = 0; i < numOfUniqueSrcs; i++) {
			for (int j = 0; j < partDegs[i]; j++) {
				partArray[i][j] = -1;
			}
		}
	}
}
