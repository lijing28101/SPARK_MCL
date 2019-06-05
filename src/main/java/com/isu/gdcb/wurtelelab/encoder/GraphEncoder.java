package com.isu.gdcb.wurtelelab.encoder;

import java.io.Serializable;

/**
 * Created by nishanth on 4/12/17.
 */
public abstract class GraphEncoder implements Serializable{

    public abstract String getVertexString(long vertexId, String vertexName, int groupId);

    public abstract String getEdgeString(long vertex_1, long vertex_2, double weight);
}
