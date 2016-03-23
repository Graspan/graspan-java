package edu.uci.ics.cs.graspan.edgecomputation;

public class ArrayPointerMadness {
	public static void main(String args[]) {
		int edges[] = new int[2];
		edges[0] = 0;
		edges[1] = 1;

		int newtargets[] = edges;
		int op[] = new int[4];
		op[0] = -1;
		op[1] = -2;
		op[2] = -3;
		op[3] = -4;

		int oldtargets[] = newtargets;

		newtargets = op;
		edges = null;

		System.out
				.println(newtargets[0] + " " + oldtargets[0] + " " + edges[0]);

	}
}
