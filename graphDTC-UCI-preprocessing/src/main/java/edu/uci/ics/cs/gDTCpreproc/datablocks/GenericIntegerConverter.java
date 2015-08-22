package edu.uci.ics.cs.gDTCpreproc.datablocks;

/**
 * ah46.
 *Converts an Integer to a byte array of custom size.
 */
public class GenericIntegerConverter {
	int numBytes;
	
	public GenericIntegerConverter(int numBytes)
	{
		this.numBytes=numBytes;
	}
	
	public Integer getValue(byte[] array) {
		Integer val = 0;
		for (int i = 0; i < numBytes; i++) {
			val = val + ((array[i] & 0xff) << i * 8);
		}
		return val;
	}

	public void setValue(byte[] array, Integer value) {
		for (int i = 0; i < numBytes; i++) {
			array[i] = (byte) (((value) >>> i * 8) & 0xff);
		}
	}
	
	public int getSize(){
		return numBytes;
	}

}