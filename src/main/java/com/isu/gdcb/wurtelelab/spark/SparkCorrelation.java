package com.isu.gdcb.wurtelelab.spark;

import com.isu.gdcb.wurtelelab.dataframe.DataFrameBuilder;
import com.isu.gdcb.wurtelelab.math.AbstractCorrelation;
import com.isu.gdcb.wurtelelab.math.PearsonCorrelation;
import com.isu.gdcb.wurtelelab.math.RankCorrelation;
import com.isu.gdcb.wurtelelab.model.CorrelationVectorKey;
import com.isu.gdcb.wurtelelab.util.FileUtil;
import com.isu.gdcb.wurtelelab.util.StringUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by nishanth on 2/17/17.
 */
public class SparkCorrelation implements Serializable {

    private int numPartitions           =   31;
    private double adjacencyThreshold   =   0.7;
    private int corr_method             =   0; // pearson
    private final String R_VALUE_FOLDER_NAME = "r_value";
    private int isLog2                  =   1;

    private AbstractCorrelation correlation;

    public SparkCorrelation(int numPartitions, double adjacencyThreshold, int corr_method, int isLog2){
        if(adjacencyThreshold < 0.5){
            System.out.println("Adjacency Threshold is too low. Please provide value above 0.5.");
            return;
        }

        this.numPartitions      =   numPartitions;
        //this.numPartitions      =   5;
        this.corr_method        =   corr_method;
        this.adjacencyThreshold =   adjacencyThreshold;
        this.isLog2             =   isLog2;

        if(this.corr_method == 1) this.correlation = new RankCorrelation();
        else this.correlation = new PearsonCorrelation();
    }

    public void computeCorrelation(SparkConf sparkConf, JavaSparkContext javaSparkContext, String inputFilepath, String outputFilePath, String verticesFilePath) {

        JavaRDD<String> csvRDD = javaSparkContext.textFile(inputFilepath);
        csvRDD = javaSparkContext.parallelize(csvRDD.collect(), this.numPartitions);
        JavaPairRDD<String, Vector> vectorJavaRDD = convertStringRDDToVectorRDD(csvRDD);

        JavaPairRDD<String, Vector> filteredVectorJavaRDD = filterNullVectors(vectorJavaRDD);
        filteredVectorJavaRDD = filteredVectorJavaRDD.sortByKey(true);

        //Save vertices in a separate file
        FileUtil.saveStringRDDAsFile(filteredVectorJavaRDD.keys(), verticesFilePath);

        final List<Tuple2<String, Vector>> referenceVectors = filteredVectorJavaRDD.collect();
        javaSparkContext.parallelize(referenceVectors, this.numPartitions);

        JavaPairRDD<CorrelationVectorKey, Vector> correlationRDD = filteredVectorJavaRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Vector>,  CorrelationVectorKey, Vector>() {
            public Iterator<Tuple2<CorrelationVectorKey, Vector>> call(Tuple2<String, Vector> tuple) throws Exception {
                Iterator<Tuple2<String, Vector>> localIter = referenceVectors.iterator();
                int i = 0;
                double p_value = 0;
                double[] rValueArray     =   new double[referenceVectors.size()];
                double[] pValueArray     =   new double[referenceVectors.size()];
                while (localIter.hasNext()) {
                    Tuple2<String, Vector> referenceVector = localIter.next();
                    double r_value = correlation.getCorrelation(tuple._2().toArray(), referenceVector._2().toArray(), isLog2);

                    if (r_value < adjacencyThreshold) r_value = 0;
                    //else r_value = 1;

                    rValueArray[i] = StringUtil.getThirdPrecisionValue(r_value);
                    pValueArray[i++] = p_value;
                }

                CorrelationVectorKey rVectorKey = new CorrelationVectorKey(tuple._1(),"r");
                Vector rValueVector   =   Vectors.dense(rValueArray);
                CorrelationVectorKey pVectorKey = new CorrelationVectorKey(tuple._1(),"p");
                Vector pValueVector   =   Vectors.dense(pValueArray);
                CorrelationVectorKey qVectorKey = new CorrelationVectorKey(tuple._1(),"q");
                Vector qValueVector   =   Vectors.dense(pValueArray);

                List<Tuple2<CorrelationVectorKey,Vector>> correlationVectors = new ArrayList<Tuple2<CorrelationVectorKey, Vector>>();
                correlationVectors.add(new Tuple2<CorrelationVectorKey, Vector>(rVectorKey,rValueVector));
                correlationVectors.add(new Tuple2<CorrelationVectorKey, Vector>(pVectorKey,pValueVector));
                correlationVectors.add(new Tuple2<CorrelationVectorKey, Vector>(qVectorKey,qValueVector));

                return correlationVectors.iterator();
            }
        });

        JavaPairRDD<String, Vector> rValueCorrelationRDD = this.getCorrelationVectorsByValueType(correlationRDD, "r");
        FileUtil.saveCorrelationRDDAsFile(rValueCorrelationRDD, outputFilePath+ File.separator+R_VALUE_FOLDER_NAME);
    }

    private JavaPairRDD<String, Vector> getCorrelationVectorsByValueType(JavaPairRDD<CorrelationVectorKey, Vector> correlationRDD, final String valueType){
        JavaPairRDD<String, Vector> filteredCorrelationRDD = correlationRDD.mapToPair(new PairFunction<Tuple2<CorrelationVectorKey, Vector>, String, Vector>() {
            public Tuple2<String, Vector> call(Tuple2<CorrelationVectorKey, Vector> rValueCorrelation) throws Exception {
                if(rValueCorrelation._1().getValueType().equals(valueType)){
                    return new Tuple2<String, Vector>(rValueCorrelation._1().getVectorName(),rValueCorrelation._2());
                }
                return null;
            }
        });
        return filteredCorrelationRDD;
    }

    private Dataset<Row> createDataFrame(JavaPairRDD<String, Vector> correlationRDD) {

        JavaRDD<Row> rowRDD = correlationRDD.map(new Function<Tuple2<String, Vector>, Row>() {
            public Row call(Tuple2<String, Vector> tuple) throws Exception {
                List<Object> fieldList  =   new ArrayList<Object>();
                fieldList.add(tuple._1());
                for (double d : tuple._2().toArray()) {
                    fieldList.add(d + "");
                }
                return RowFactory.create(fieldList.toArray());
            }
        });
        List<String> colNames   =   new ArrayList<String>();
        colNames.add("Samples");
        colNames.addAll(correlationRDD.keys().collect());

        DataFrameBuilder dataFrameBuilder   =   new DataFrameBuilder(colNames, rowRDD);
        return dataFrameBuilder.buildDataFrame();
    }

    private JavaPairRDD<String, Vector> filterNullVectors(JavaPairRDD<String, Vector> rowVectors) {

        JavaPairRDD<String, Vector> filteredVectors = rowVectors.filter(new Function<Tuple2<String, Vector>, Boolean>() {
            public Boolean call(Tuple2<String, Vector> stringVectorTuple2) throws Exception {
                if (stringVectorTuple2 != null && stringVectorTuple2._2() != null)  return true;
                return false;
            }
        });
        return filteredVectors;
    }

    private JavaPairRDD<String, Vector> convertStringRDDToVectorRDD(JavaRDD<String> csvRDD) {

        JavaPairRDD<String, Vector> keyVectorRDD = csvRDD.mapToPair(new PairFunction<String, String, Vector>() {
            public Tuple2<String, Vector> call(String s) throws Exception {
                String[] fields = s.split(",");
                if (fields.length > 0) {
                    String key = fields[0];
                    try {
                        //TODO Create a data frame with all gene names and their corresponding index.
                        //double val = Double.parseDouble(fields[1]);
                        double[] values = new double[(fields.length - 1)];
                        for (int i = 0; i < fields.length - 1; i++) {
                            values[i] = Double.parseDouble(fields[i + 1]);
                        }
                        Vector rowVector = Vectors.dense(values);

                        return new Tuple2<String, Vector>(key, rowVector);
                    } catch (NumberFormatException nfe) {
                        System.out.println(nfe.getMessage());
                        return null;
                    }
                }
                return null;
            }
        });
        return keyVectorRDD;
    }
}