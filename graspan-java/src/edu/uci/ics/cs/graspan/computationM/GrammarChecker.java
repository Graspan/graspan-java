package edu.uci.ics.cs.graspan.computationM;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GrammarChecker {
	
	public static Map<Byte, HashMap<Byte, Byte>> dRules = new HashMap<Byte, HashMap<Byte, Byte>>();
	
	public static Map<Byte, Byte> sRules = new HashMap<Byte, Byte>();
	
	public static Set<Byte> eRules = new HashSet<Byte>();
	
	
	public static void loadGrammars(File grammar_input) throws IOException {
		// initialize edgeDestCount and partSizes variables

		/*
		 * Scan the grammar file
		 */
		BufferedReader inGrammarStrm = new BufferedReader(
				new InputStreamReader(new FileInputStream(grammar_input)));
		String ln;

		String[] tok;
		while ((ln = inGrammarStrm.readLine()) != null) {
			tok = ln.split("\t");
			if (tok.length == 1) { // production with 1 symbol (self)
				eRules.add((byte) Integer.parseInt(tok[0]));
				continue;
			}
			if (tok.length == 2) { // production with 2 symbols
				sRules.put((byte) Integer.parseInt(tok[0]), (byte) Integer.parseInt(tok[1]));
				continue;
			}
			if (tok.length == 3) { // production with 3 symbols
			// consider production form : BC ----- > A (Map will store them as (B,(C,A)))
				HashMap<Byte, Byte> destValOPValPair=new HashMap<Byte, Byte>();
				destValOPValPair.put((byte) Integer.parseInt(tok[1]), (byte) Integer.parseInt(tok[2]));
				dRules.put((byte) Integer.parseInt(tok[0]), destValOPValPair);
			}
		}

		inGrammarStrm.close();
		// logger.info("Loaded " + ".grammar");
	}
	
	
	public static byte checkGrammar(byte srcEval, byte destEval){
		// BC ----- > A : <B,<C,A>> : <srcEval,<destEval,OPEval>>
		byte OPEval = -1;
		HashMap<Byte, Byte> destValOPValPair = dRules.get(srcEval);
		if (destValOPValPair.get(destEval) != null)
			OPEval = destValOPValPair.get(destEval);
		return OPEval;
	}
	

}
