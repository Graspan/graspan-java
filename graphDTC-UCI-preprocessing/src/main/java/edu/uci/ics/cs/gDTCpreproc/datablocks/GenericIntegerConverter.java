package edu.uci.ics.cs.gDTCpreproc.datablocks;

/**
 * ah46.
 *Converts an Integer to a byte array of custom size.
 */
public class GenericIntegerConverter implements BytesToValueConverter<Integer>{

	public Integer getValue(byte[] array) {
		Integer val = 0;
		for (int i = 0; i < array.length; i++) {
			val = val + ((array[i] & 0xff) << i * 8);
		}
		return val;
	}

	public void setValue(byte[] array, Integer value) {
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) (((value) >>> i * 8) & 0xff);
		}
	}

	@Override
	public int sizeOf() {
		// TODO Auto-generated method stub
		return 0;
	}

}