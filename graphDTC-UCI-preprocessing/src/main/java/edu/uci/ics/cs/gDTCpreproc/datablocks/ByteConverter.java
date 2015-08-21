package edu.uci.ics.cs.gDTCpreproc.datablocks;

public class ByteConverter {
	 public int sizeOf() {
	        return 1;
	    }

	    public Integer getValue(byte[] array) {
	        return ((array[3]  & 0xff) << 24) + ((array[2] & 0xff) << 16) + ((array[1] & 0xff) << 8) + (array[0] & 0xff);
	    }

	    public void setValue(byte[] array, Integer x) {
	        array[0] = (byte) ((x) & 0xff);
	        array[1] = (byte) ((x >>> 8) & 0xff);
	        array[2] = (byte) ((x >>> 16) & 0xff);
	        array[3] = (byte) ((x >>> 24) & 0xff);

	    }
}
