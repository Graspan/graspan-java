package edu.uci.ics.cs.graspan.edgecomputation;

import java.util.HashSet;

public class removeSet {

	public static void main(String args[]) {

		HashSet<Byte> myset = new HashSet<Byte>();
		
		myset.remove(8);
		System.out.println(myset.remove(8));
	}
}
