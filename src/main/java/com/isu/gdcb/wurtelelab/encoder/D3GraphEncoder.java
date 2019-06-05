package com.isu.gdcb.wurtelelab.encoder;

/**
 * Created by nishanth on 4/12/17.
 */
public class D3GraphEncoder extends GraphEncoder {

    public String getVertexString(long vertexId, String vertexName, int groupId) {
        return ""+ vertexId+","+ vertexName +","+ groupId;
    }

    public String getEdgeString(long vertex_1, long vertex_2, double weight) {
        return ""+vertex_1+","+vertex_2+","+weight;
    }
}
