package edu.uci.ics.cs.gDTCpreproc.preprocessing;

import org.junit.Test;

import edu.uci.ics.cs.gDTCpreproc.datablocks.FloatConverter;
import edu.uci.ics.cs.gDTCpreproc.preprocessing.FastSharder;

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
            long l  = FastSharder.packEdges(j, i);
            assertEquals(j, FastSharder.getFirst(l));
            assertEquals(i, FastSharder.getSecond(l));

            long k  = FastSharder.packEdges(i, j);
            assertEquals(i, FastSharder.getFirst(k));
            assertEquals(j, FastSharder.getSecond(k));
        }
    }

}
