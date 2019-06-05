package com.isu.gdcb.wurtelelab.model;

import java.io.Serializable;

/**
 * Created by nishanth and mhhur on 5/2/2017.
 */
public class CorrelationVectorKey implements Serializable{
    private String valueType;
    private String vectorName;

    public CorrelationVectorKey(String vectorName, String valueType){
        this.vectorName = vectorName;
        this.valueType = valueType;
    }

    public String getVectorName() {
        return vectorName;
    }

    public void setVectorName(String vectorName) {
        this.vectorName = vectorName;
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    @Override
    public int hashCode() {
        return (valueType+vectorName).hashCode();
    }
}
