package edu.uci.ics.cs.graspan.experiments;

import java.io.IOException;

public class GPCppResultExtractorClient {
	
	// args[0] - io file
	// args[1] - total time
	// args[2] - total preprocessing time
	// args[3] - cpp.output
	public static void main(String args[]) throws IOException{
		
		double total_time = Double.parseDouble(args[1]);
		double pp_time=Double.parseDouble(args[2]);
		
		Extractor ex = new Extractor();
		double total_IO_time = ex.scan_IO_times(args[0], total_time, pp_time);
		
		System.out.println("Total IO time during computation= "+total_IO_time);
		
		ex.cpp_outputScanner(args[3]);
	}

}
