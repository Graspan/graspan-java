package gDTCpreproc.vertexdata;

/**
 * Callback object used when iterating vertex-values with VertexAggregator
 * @author akyrola
 * @see gDTCpreproc.vertexdata.VertexAggregator
 */
public interface ForeachCallback <VertexDataType> {

    /**
     * Called for each vertex
     * @param vertexId
     * @param vertexValue
     */
   public void callback(int vertexId, VertexDataType vertexValue);

}
