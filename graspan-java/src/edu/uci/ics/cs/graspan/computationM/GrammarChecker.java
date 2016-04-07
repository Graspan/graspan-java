package edu.uci.ics.cs.graspan.computationM;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GrammarChecker {
	
	public static Map<Byte, HashMap<Byte, Byte>> dRules = new HashMap<Byte, HashMap<Byte, Byte>>();
	
	public static Map<Byte, Byte> sRules = new HashMap<Byte, Byte>();
	
	public static Set<Byte> eRules = new HashSet<Byte>();
	
	
	public static void loadGrammars(File grammar_input){
		
	}
	
	
	public static byte checkGrammar(byte src1Eval, byte src2Eval){
		byte dstEval = -1;
		
		
		return dstEval;
	}
	

}
