package com.isu.gdcb.wurtelelab.math;

import java.io.Serializable;

/**
 * Created by nishanth and Manhoi Hur on 4/25/2017.
 */
public abstract class AbstractCorrelation implements Serializable {

    public abstract double getCorrelation(double[] X, double[] Y, int isLog2);

    public abstract double getPvalue(final double corr, final int n);

    public abstract double getPvalue(final double corr, final double n);

    /**
     * log(2), used in log2().
     */
    private static final double LOG2 = java.lang.Math.log(2);

    /**
     * Log of base 2.
     */
    public static double log2(double x) {
        return java.lang.Math.log(x) / LOG2;
    }
}