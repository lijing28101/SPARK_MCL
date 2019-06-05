package com.isu.gdcb.wurtelelab.math;

/**
 * Created by mhhur on 4/21/2017.
 */
import java.util.List;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

/**
 * Created by mhhur on 3/10/17.
 */
public class PearsonCorrelation extends AbstractCorrelation{
    final static PearsonsCorrelation PC = new PearsonsCorrelation();

    public double getCorrelation(double [] X, double [] Y, int isLog2) {
        if (isLog2 == 1) {
            for (int i=0; i<X.length; i++) {
                X[i] = log2(X[i]);
                Y[i] = log2(Y[i]);
            }
        }
        return PC.correlation(X, Y);
    }

    public double getPvalue(final double corr, final int n) {
        return getPvalue(corr, (double) n);
    }

    public double getPvalue(final double corr, final double n) {
        double t = Math.abs(corr * Math.sqrt( (n-2.0) / (1.0 - (corr * corr)) ));
        TDistribution tdist = new TDistribution(n-2);
        double pvalue = 2* (1.0 - tdist.cumulativeProbability(t));
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