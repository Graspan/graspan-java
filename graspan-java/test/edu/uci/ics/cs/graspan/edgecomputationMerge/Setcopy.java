package edu.uci.ics.cs.graspan.edgecomputationMerge;

import java.util.HashSet;

public class Setcopy {

	public static void main (String args[]){
		HashSet<Integer> set1=new HashSet<Integer>();
		set1.add(1);
		set1.add(2);
		
		System.out.println(set1);
		
		HashSet<Integer> set2=new HashSet<Integer>(set1);
		
		set2.remove(2);
		
		System.out.println("set1"+set1);
		System.out.println("set2"+set2);
	}
	
	
}
