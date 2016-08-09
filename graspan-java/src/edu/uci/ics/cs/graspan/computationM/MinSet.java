package edu.uci.ics.cs.graspan.computationM;

import java.util.HashSet;

public class MinSet {

	// always points to the minimum element
	private int ptr;

	private final int minSetId;
	
	private int currentVId;
	
	private HashSet<Byte> evals;
	
	private byte[] evals_arr;

	public int getMinSetId() {
		return minSetId;
	}

	public MinSet(int minSetId) { //TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. :DONE
		ptr = 0;
		this.minSetId = minSetId;
		
		//evals init - old hashset implementation
//		evals = new HashSet<Byte>(); //TODO: NEED TO FIX. IDENTIFIED AS GC-EXPENSIVE BY YOURKIT. :DONE
		
		//evals init - new array implementation
		evals_arr = new byte[GrammarChecker.getNumOfGrammarSymbols()];
		for (int i = 0; i < evals_arr.length; i++) {
			evals_arr[i] = -1;
		}
	}

	public int getCurrentVId() { 
		return currentVId;
	}

	public void setCurrentVId(int id) {
		currentVId = id;
	}

	public int getPtr() {
		return ptr;
	}
	
	public void incrementPtr() {
		ptr++;
	}

	//adding edge val - old hashset implementation
	public void addEval(byte eval) { //TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
		evals.add(eval);
	}
	
	//adding edge val - new array implementation
	public void addToEvalArr(byte eval) { 
		
		//ensure it does not already exist
		for (int i = 0; i < evals_arr.length; i++) {
			if (evals_arr[i] == eval) {
				return;
			}
			if (evals_arr[i] == -1) {
				break;
			}
		}
		
		for (int i = 0; i < evals_arr.length; i++) {
			if (evals_arr[i] == -1) {
				evals_arr[i] = eval;
				break;
			}
		}
	}

	//clearing edge vals - old hashset implementation
	public void clearEvalSet() { //TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
		evals.clear();
	}
	
	//clearing edge vals - new array implementation
	public void clearEvalArr() { 
		for (int i = 0; i < evals_arr.length; i++) {
			evals_arr[i] = -1;
		}
	}

	//return edge vals - old hashset implementation
	public HashSet<Byte> getEvals() { //TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
		return evals;
	}
	
	//return edge vals - new array implementation
	public byte[] getEvalsArr(){
		return evals_arr;
	}

	@Override
	public String toString() { //TODO: NEED TO UPDATE FOR EVALS AS ARRAY :DONE
		
		String evals="";
		for (int i = 0; i < evals_arr.length; i++) {
			if (evals_arr[i] == -1)
				break;
			evals += evals_arr[i] + " ";
		}
		evals.trim();
		
		String s = "";
		s = "MinSet no. " + this.minSetId + ": " + this.getCurrentVId() + " " + "Evals : <"+evals+">";
//		s = "MinSet no. " + this.minSetId + ": " + this.getCurrentVId() + " " + this.getEvals();
		return s;
	}

}
