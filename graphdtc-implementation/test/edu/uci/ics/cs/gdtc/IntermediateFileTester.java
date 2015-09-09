package edu.uci.ics.cs.gdtc;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class IntermediateFileTester {

	public static void main(String args[]) throws IOException {
		String baseFilename = args[0];
		readPartitionFile(baseFilename);
	}

	private static void readPartitionFile(String baseFilename) throws FileNotFoundException {
		DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFilename)));

		while (true) {
			try {
				// srcVId
				System.out.println(dataIn.readInt());

				// count (number of destVts from srcV in the current list)
				int count = dataIn.readInt();
				System.out.println(count);

				for (int i = 0; i < count; i++) {
					// edge Value
					System.out.println(dataIn.readInt());

					// edge value
					System.out.println(dataIn.readByte());
				}

			} catch (Exception exception) {
				break;
			}
		}
	}
}
