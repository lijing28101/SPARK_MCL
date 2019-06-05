package com.isu.gdcb.wurtelelab.math;

/**
 * Created by Manhoi Hur on 3/2/17.
 */
public class StandardPearsonCorrelation {

    public static double getCorrelation(double[] x, double[] y) {
        double r = 0;
        double xmean = 0, ymean = 0;
        double xavg = 0, yavg = 0;
        double covxy = 0, covxx = 0, covyy = 0;
        int df = 0;

        int xSize = 0;
        int ySize = 0;
        int size = x.length;
        for (int i = 0; i < size; i++) {
            xmean += x[i];
            ymean += y[i];

            xavg += (x[i]);
            yavg += (y[i]);
            if (x[i] > 0) xSize++;
            if (y[i] > 0) ySize++;
        }

        if (xSize != ySize) {
            df = xSize + ySize - 2;
            xmean /= xSize;
            ymean /= ySize;

            xavg /= xSize;
            yavg /= ySize;
        } else {
            xmean /= size;
            ymean /= size;

            xavg /= size;
            yavg /= size;

            df = xSize + ySize - 2;
        }

        for (int i = 0; i < size; i++) {
            covxy += ((x[i] - xmean) * (y[i] - ymean));
            covxx += ((x[i] - xmean) * (x[i] - xmean));
            covyy += ((y[i] - ymean) * (y[i] - ymean));
        }
        //*fc = ymean - xmean;
                /*
                if (xavg != static_cast<double>(xavg))
                {
                    *s1avg = 0;
                }
                else {
                    *s1avg = xavg;
                }

                if (yavg != static_cast<double>(yavg))
                {
                    *s2avg = 0;
                }
                else {
                    *s2avg = yavg;
                }
                */
        double sx = 0, sy = 0;
        double varx = 0.0, vary = 0.0;
        double epx = 0.0, epy = 0.0;
        for (int i = 0; i < size; i++) {
            sx = x[i] - (xmean);
            sy = y[i] - (ymean);
            epx += sx;
            epy += sy;
            varx += sx * sx;
            vary += sy * sy;
        }

        varx = (varx - epx * epx / (double) size) / (double) (size - 1); //Corrected two-pass formula (14.1.8).
        vary = (vary - epy * epy / (double) size) / (double) (size - 1); //Corrected two-pass formula (14.1.8).

        r = covxy / Math.sqrt(covxx * covyy);
        double r2 = r;
        if (r != (double) r) {
            r = 0.0;
        }

        return r;
    }
}
