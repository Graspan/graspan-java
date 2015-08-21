package edu.uci.ics.cs.gDTCpreproc.datablocks;

/**
 *Converts an Integer to a byte array of custom size.
 */
public class GenericIntegerConverter {// ah46.

	public Integer getValue(byte[] array) {
		Integer val = 0;
		for (int i = 0; i < array.length; i++) {
			val = val + ((array[i] & 0xff) << i * 8);
		}
		return val;
	}

	public void setValue(byte[] array, Integer x) {
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) (((x) >>> i * 8) & 0xff);
		}
	}
}