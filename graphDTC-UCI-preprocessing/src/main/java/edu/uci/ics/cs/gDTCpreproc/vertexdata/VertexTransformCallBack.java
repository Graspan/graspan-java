package edu.uci.ics.cs.gDTCpreproc.vertexdata;

/**
 * Transforms vertex value to another
 * @see edu.uci.ics.cs.gDTCpreproc.vertexdata.VertexTransformer
 */
public interface VertexTransformCallBack<VertexDataType>  {

    VertexDataType map(int vertexId, VertexDataType value);

}
