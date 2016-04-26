package edu.uci.ics.cs.graspan.support;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * 
 * @author aftab
 * Src: http://stackoverflow.com/questions/9027317/how-to-convert-milliseconds-to-hhmmss-format
 */
public class GraspanTimer {
	private long start;
	private long duration=-1;
	private static final Logger logger = GraspanLogger.getLogger("GraspanTimer");
	
	public GraspanTimer(long start){
		this.start = start;
	}
	
	public long getDuration(){
		if (duration==-1){
			logger.info("Error: Duration is -1!");
//			System.exit(0);
		}
		return duration;
	}
	
	public void calculateDuration(long stop){
		this.duration = stop - this.start;
//	    String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(duration),
//	            								     TimeUnit.MILLISECONDS.toMinutes(duration) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
//	            								     TimeUnit.MILLISECONDS.toSeconds(duration) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)));
//		return hms;
//		return duration;
	}
	
	public static String getDurationInHMS(long duration){
	String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(duration),
			     TimeUnit.MILLISECONDS.toMinutes(duration) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
			     TimeUnit.MILLISECONDS.toSeconds(duration) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)));
	return hms;
	}
	
	

}
