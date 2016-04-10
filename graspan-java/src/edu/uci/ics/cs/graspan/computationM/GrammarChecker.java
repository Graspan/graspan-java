package edu.uci.ics.cs.graspan.computationM;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class GrammarChecker {
	//sort the mapping info of inputed grammar with used grammar
	private static Map<String, Byte> map = new LinkedHashMap<String, Byte>();
	
	private static Map<Byte, String> reverse_map =new LinkedHashMap<Byte, String>();
	
	private static Map<Byte, HashMap<Byte, Byte>> dRules = new LinkedHashMap<Byte, HashMap<Byte, Byte>>();
	
	private static Map<Byte, Byte> sRules = new LinkedHashMap<Byte, Byte>();
	
	public static Set<Byte> eRules = new LinkedHashSet<Byte>();
	
	
	public static void loadGrammars(File grammar_input) throws IOException {
		// initialize edgeDestCount and partSizes variables
		//Scan the grammar file
		BufferedReader inGrammarStrm = new BufferedReader(new FileReader(grammar_input));
		String ln;

		String[] tok;
		while ((ln = inGrammarStrm.readLine()) != null) {
			tok = ln.split("\t");
			if (tok.length == 1) { // production with 1 symbol (self)
				eRules.add(getValue(tok[0]));
			}
			else if (tok.length == 2) { // production with 2 symbols
				sRules.put(getValue(tok[1]), getValue(tok[0]));
			}
			else if (tok.length == 3) { // production with 3 symbols
			// consider production form : A->BC (Map will store them as (B,(C,A)))
				byte src1 = getValue(tok[1]);
				byte src2 = getValue(tok[2]);
				byte dst = getValue(tok[0]);
				
				if(dRules.containsKey(src1)){
					HashMap<Byte, Byte> mapR = dRules.get(src1);
					assert(!mapR.containsKey(src2));
					mapR.put(src2, dst);
				}
				else{
					HashMap<Byte, Byte> mapR = new HashMap<Byte, Byte>();
					mapR.put(src2, dst);
					dRules.put(src1, mapR);
				}
			}
			else{
				throw new RuntimeException("Wrong length in the grammar file!!!");
			}
		}

		inGrammarStrm.close();
		
		writeCollection(new ArrayList<Map.Entry<String, Byte>>(map.entrySet()), new File(grammar_input.getParentFile(), "grammar"));
		// logger.info("Loaded " + ".grammar");
	}
	
	
	public static Byte getValue(String string) {
		// TODO Auto-generated method stub
		string = string.trim();
		if(map.containsKey(string)){
			return map.get(string);
		}
		else{
			byte e = (byte) map.size();
			map.put(string, e);
			reverse_map.put(e, string);
			return e;
		}
	}
	
	public static String getValue(Byte e){
		return reverse_map.get(e);
	}


	public static byte checkL2Rules(byte srcEval, byte destEval){
		// BC ----- > A : <B,<C,A>> : <srcEval,<destEval,OPEval>>
		byte OPEval = -1;
		
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
	
	public static <T> void writeCollection(Collection<T> collection, File file){
		PrintWriter out = null;
		try{
//			if (!file.getParentFile().exists()) {
//				file.getParentFile().mkdirs();
//			}
			//write the passing inputs
			out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
			for(T element: collection){
				out.println(element);
			}
			out.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		finally{
			if(out != null)
				out.close();
		}
	}
	
	public static void main(String[] args) throws IOException {
		GrammarChecker.loadGrammars(new File("graph.grammar"));
		System.out.println(dRules);
		System.out.println(sRules);
		System.out.println(eRules);
	}
}
