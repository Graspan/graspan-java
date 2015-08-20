package edu.uci.ics.cs.gDTCpreproc.preprocessing;

import org.junit.Test;

import edu.uci.ics.cs.gDTCpreproc.datablocks.FloatConverter;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.PartitionGenerator;

import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 *
 */
public class TestIdPacking {

    @Test
    public void testPacking() {
        Random r = new Random();
        for(int j=0; j < 100000; j++) {
            int i = r.nextInt(Integer.MAX_VALUE);
            long l  = PartitionGenerator.packEdges(j, i);
            assertEquals(j, PartitionGenerator.getFirst(l));
            assertEquals(i, PartitionGenerator.getSecond(l));

            long k  = PartitionGenerator.packEdges(i, j);
            assertEquals(i, PartitionGenerator.getFirst(k));
            assertEquals(j, PartitionGenerator.getSecond(k));
        }
    }

}
