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
		BufferedReader inGrammarStrm = new BufferedReader(new InputStreamReader(new FileInputStream(grammar_input)));
		String ln;

		String[] tok;
		while ((ln = inGrammarStrm.readLine()) != null) {
			tok = ln.split("\t");
			if (tok.length == 1) { // production with 1 symbol (self)
//				eRules.add((byte) Integer.parseInt(tok[0]));
				eRules.add(Byte.parseByte(tok[0]));
			}
			else if (tok.length == 2) { // production with 2 symbols
//				sRules.put((byte) Integer.parseInt(tok[0]), (byte) Integer.parseInt(tok[1]));
				sRules.put(Byte.parseByte(tok[0]), Byte.parseByte(tok[1]));
			}
			else if (tok.length == 3) { // production with 3 symbols
			// consider production form : BC ----- > A (Map will store them as (B,(C,A)))
//				HashMap<Byte, Byte> destValOPValPair=new HashMap<Byte, Byte>();
//				destValOPValPair.put((byte) Integer.parseInt(tok[1]), (byte) Integer.parseInt(tok[2]));
//				dRules.put((byte) Integer.parseInt(tok[0]), destValOPValPair);
				
				byte src1 = Byte.parseByte(tok[0]);
				byte src2 = Byte.parseByte(tok[1]);
				byte dst = Byte.parseByte(tok[2]);
				
				if(dRules.containsKey(src1)){
					HashMap<Byte, Byte> map = dRules.get(src1);
					assert(!map.containsKey(src2));
					
					map.put(src2, dst);
				}
				else{
					HashMap<Byte, Byte> map = new HashMap<Byte, Byte>();
					map.put(src2, dst);
					dRules.put(src1, map);
				}
			}
			else{
				throw new RuntimeException("Wrong length in the grammar file!!!");
			}
		}

		inGrammarStrm.close();
		// logger.info("Loaded " + ".grammar");
	}
	
	
	public static byte checkL2Rules(byte srcEval, byte destEval){
		// BC ----- > A : <B,<C,A>> : <srcEval,<destEval,OPEval>>
		byte OPEval = -1;
		
//		HashMap<Byte, Byte> destValOPValPair = dRules.get(srcEval);
//		if (destValOPValPair.get(destEval) != null)
//			OPEval = destValOPValPair.get(destEval);
		
		if(dRules.containsKey(srcEval)){
			HashMap<Byte, Byte> map = dRules.get(srcEval);
			if(map.containsKey(destEval)){
				OPEval = map.get(destEval);
			}
		}
		
		return OPEval;
	}
	

	public static byte checkL1Rules(byte src){
		byte OPEval = -1;
		if(GrammarChecker.sRules.containsKey(src)){
			OPEval = GrammarChecker.sRules.get(src);
		}
		return OPEval;
	}
}
