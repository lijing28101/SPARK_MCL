package com.isu.gdcb.wurtelelab.math;

/**
 * Created by mhhur on 3/13/17.
 */
import java.util.List;
import java.lang.*;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

/**
 * Created by Manhoi Hur on 3/2/17.
 */
// Spearman's Rank Correlation
public class RankCorrelation extends AbstractCorrelation{

    final static SpearmansCorrelation SC = new SpearmansCorrelation();

    public double getCorrelation(double [] X, double [] Y, int isLog2) {
        if (isLog2 == 1) {
            for (int i=0; i<X.length; i++) {
                X[i] = log2(X[i]);
                Y[i] = log2(Y[i]);
            }
        }
        return SC.correlation(X, Y);
    }

    public double getPvalue(final double corr, final int n) {
        return getPvalue(corr, (double) n);
    }

    public double getPvalue(double corr, double n) {
        double t = Math.abs(corr * Math.sqrt( (n-2.0) / (1.0 - (corr * corr)) ));
        TDistribution tdist = new TDistribution(n-2);
        double pvalue = 2.0 * (1.0 - tdist.cumulativeProbability(t));
        return pvalue;
    }

    public static double[] toDoubleArray(List<Double> list) {
        double[] arr = new double[list.size()];
        for (int i=0; i < list.size(); i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }
}
