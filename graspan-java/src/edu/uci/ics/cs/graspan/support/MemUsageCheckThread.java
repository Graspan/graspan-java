package edu.uci.ics.cs.graspan.support;

import java.text.NumberFormat;
import java.util.logging.Logger;

import edu.uci.ics.cs.graspan.computationM.EngineM;

public class MemUsageCheckThread extends Thread {
	private static final Logger logger = GraspanLogger
			.getLogger("MemUsageCheckThread");

	public void run() {
		Runtime runtime = Runtime.getRuntime();
		NumberFormat format = NumberFormat.getInstance();

		while (true) {
			long maxMemory = runtime.maxMemory();
			long allocatedMemory = runtime.totalMemory();
			long freeMemory = runtime.freeMemory();
			logger.info("Free memory (MB): "
					+ format.format(freeMemory / 1048576));
			logger.info("Allocated memory (MB): "
					+ format.format(allocatedMemory / 1048576));
			logger.info("Max memory (MB): "
					+ format.format(maxMemory / 1048576));
			logger.info("Total free memory (MB): "
					+ format.format((freeMemory + (maxMemory - allocatedMemory)) / 1048576));

			if (((freeMemory + (maxMemory - allocatedMemory)) / 1048576) > 0) {
				EngineM.memFull = true;
			}

			try {
				this.sleep(4000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}// milliseconds
		}
	}
}
