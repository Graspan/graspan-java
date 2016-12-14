package edu.uci.ics.cs.graspan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapTest {

	Map<Integer, HashSet<MyIntObj>> diffMap = new HashMap<Integer, HashSet<MyIntObj>>();

	public MapTest() {
	}

	private void run() {
		HashSet<MyIntObj> st = new HashSet<MyIntObj>();
		st.add(new MyIntObj(5));
		st.add(new MyIntObj(7));
		diffMap.put(1, st);

		HashSet<MyIntObj> stextract2=new HashSet<MyIntObj>();
		for (int id : diffMap.keySet()) {
			HashSet<MyIntObj> stextract = diffMap.get(id);
			for (MyIntObj intObj: stextract){
				stextract2.add(intObj);
			}
		}
		
		System.out.println("stextract2.size() "+stextract2.size());
		MyIntObj mio=new MyIntObj(5);
		stextract2.remove(mio);
		System.out.println("stextract2.size() after removing object: "+stextract2.size());
		
		System.out.println(diffMap.keySet().size());
		for (int id : diffMap.keySet()) {
			HashSet<MyIntObj> stextract = diffMap.get(id);
			for (MyIntObj num: stextract){
				System.out.println("id="+id+" num="+num.getInt());
			}
		}
		
		

	}

	public static void main(String args[]) {
		MapTest mpt = new MapTest();
		mpt.run();
	}

	
	private class MyIntObj {
		private int n;

		public MyIntObj(int n) {
			this.n = n;
		}
		
		public int getInt(){
			return n;
		}
		
		public int hashCode(){
			int r = 1;
			r = r * 31 + this.n;
//			r = r * 31 + this.evalue;
			return r;
		}
		
		public boolean equals(Object o){
			return (o instanceof MyIntObj) && (((MyIntObj) o).n == this.n) ;
		}
		
		
	}
}
