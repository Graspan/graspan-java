package edu.uci.ics.cs.graspan.support;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author aftab
 * Src: http://stackoverflow.com/questions/9027317/how-to-convert-milliseconds-to-hhmmss-format
 */
public class GraspanTimer {
	long start;
	long duration;
	
	public GraspanTimer(long start){
		this.start = start;
	}
	
	public long findDuration(long stop){
		this.duration = stop - this.start;
//	    String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(duration),
//	            								     TimeUnit.MILLISECONDS.toMinutes(duration) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
//	            								     TimeUnit.MILLISECONDS.toSeconds(duration) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)));
//		return hms;
		return duration;
	}
	
	public static String getDurationInHMS(long duration){
	String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(duration),
			     TimeUnit.MILLISECONDS.toMinutes(duration) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(duration)),
			     TimeUnit.MILLISECONDS.toSeconds(duration) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(duration)));
	return hms;
	}
	
	

}
