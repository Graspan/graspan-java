package edu.uci.ics.cs.graspan.support;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.EngineM;
import edu.uci.ics.cs.graspan.dispatcher.GlobalParams;

public class MemUsageCheckThread extends Thread {
	private static final Logger logger = GraspanLogger.getLogger("MemUsageCheckThread");
	public static PrintWriter memoryUsageOutput;

	public void run() {
		Runtime runtime = Runtime.getRuntime();
//		NumberFormat format = NumberFormat.getInstance();
		
		// round output info
		try {
			memoryUsageOutput = new PrintWriter(new BufferedWriter(new FileWriter(GlobalParams.getBasefilename() + ".output.memUsage.csv", true)));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		memoryUsageOutput.println("Free_memory, Allocated_memory, Max_memory, Total_free_memory");

		while (true) {
			long maxMemory = runtime.maxMemory();
			long allocatedMemory = runtime.totalMemory();
			long freeMemory = runtime.freeMemory();
			
//			logger.info("Free memory (MB): " + format.format(freeMemory / 1048576));
//			logger.info("Allocated memory (MB): " + format.format(allocatedMemory / 1048576));
//			logger.info("Max memory (MB): " + format.format(maxMemory / 1048576));
//			logger.info("Total free memory (MB): " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1048576));
			
			memoryUsageOutput.println((freeMemory / 1048576) + ","
					+ (allocatedMemory / 1048576) + "," + (maxMemory / 1048576) + ","
					+ ((freeMemory + (maxMemory - allocatedMemory)) / 1048576));
			
			if (((freeMemory + (maxMemory - allocatedMemory)) / 1048576) > 0) {
				EngineM.memFull = true;
			}

			try {
				this.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
