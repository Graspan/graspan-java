package edu.uci.ics.cs.gDTCpreproc.preprocessing;

/**
 * Interface for objects that translate edge values from string
 * to the value type.
 * @param <ValueType>
 */
public interface EdgeProcessor <ValueType>  {//ah46

    public ValueType receiveEdge(int from, int to, String token);

}
