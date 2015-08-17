package gDTCpreproc.vertexdata;

/**
 * Transforms vertex value to another
 * @see gDTCpreproc.vertexdata.VertexTransformer
 */
public interface VertexTransformCallBack<VertexDataType>  {

    VertexDataType map(int vertexId, VertexDataType value);

}
