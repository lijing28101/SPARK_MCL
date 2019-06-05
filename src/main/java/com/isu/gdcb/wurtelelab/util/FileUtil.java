package com.isu.gdcb.wurtelelab.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.io.*;
/**
 * Created by nishanth and mhhur on 3/2/17.
 */
public class FileUtil {
    private static final double CORRELATION_THRESHOLD   =   0.6;
    private static final String CORRELATION_TYPE        =   "pearson";

    public static void saveCorrelationRDDAsFile(JavaPairRDD<String, Vector> correlationRDD, String path) {
        correlationRDD = correlationRDD.filter(new Function<Tuple2<String, Vector>, Boolean>() {
            public Boolean call(Tuple2<String, Vector> tuple) throws Exception {
                if(tuple != null) return true;
                return false;
            }
        });
        correlationRDD = correlationRDD.sortByKey();
        final JavaRDD<String> stringRDD = correlationRDD.map(new Function<Tuple2<String,Vector>, String>() {
            public String call(Tuple2<String,Vector> tuple) throws Exception {
                double[] vect = tuple._2().toArray();
                StringBuilder result = new StringBuilder(tuple._1()+",");
                for(int i = 0; i < vect.length; i++){
                    if(i < vect.length - 1) result.append(vect[i]+",");
                    else result.append(vect[i]);
                }
                return result.toString();
            }
        });
        deleteIfExists(path);
        stringRDD.coalesce(1,true).saveAsTextFile(path);
    }

    private static void deleteIfExists(String path) {
        File inputPath = new File(path);
        if(inputPath.isDirectory()){
            for(File file: inputPath.listFiles()){
                deleteIfExists(file.getAbsolutePath());
            }
            inputPath.delete();
        }else
            inputPath.delete();
    }

    public static void saveStringRDDAsFile(JavaRDD<String> stringRDD, String path) {
        deleteIfExists(path);
        System.out.println("deleting "+path);
        JavaPairRDD<String, Long> verticesWithIndex = stringRDD.zipWithIndex();
        JavaPairRDD<Long, String> indexedVertices = verticesWithIndex.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return new Tuple2<Long, String>(stringLongTuple2._2(), stringLongTuple2._1());
            }
        });
        indexedVertices.coalesce(1).saveAsTextFile(path);
    }

    public static void saveClusterRDD(JavaPairRDD<Long, Iterable<Long>> clusterOutputRDD, String mclOutputPath) {
        deleteIfExists(mclOutputPath);
        clusterOutputRDD.coalesce(1,true).saveAsTextFile(mclOutputPath);
    }

    public static void saveGraphInformation(JavaRDD<String> clusterVertexCSVRDD, JavaRDD<String> clusterEdgeCSVRDD, String graphType, String graphOutputPath){
        deleteIfExists(graphOutputPath);
        deleteIfExists(graphOutputPath+File.separator+"vertex_output");
        deleteIfExists(graphOutputPath+File.separator+"edge_output");

        //
        if(graphType.equals("d3")){
            clusterEdgeCSVRDD.coalesce(1, true).saveAsTextFile(graphOutputPath+File.separator+"edge_output");
            clusterVertexCSVRDD.coalesce(1,true).saveAsTextFile(graphOutputPath+File.separator+"vertex_output");
        }else if(graphType.equals("cytoscape")){
            System.out.println("No RDD for cytoscape to save file");
        }
    }

    public static String getPartitionFilePath(String path){
        StringBuffer partitionFilePath = null;
        File inputPath = new File(path);
        if(inputPath.isDirectory()) {
            File[] files = inputPath.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.getName().startsWith("part-")) {
                        partitionFilePath = new StringBuffer();
                        partitionFilePath.append(file.getAbsolutePath());
                        return partitionFilePath.toString();
                    }
                }
            }
        }
       return null;
    }

    public static long countLinesInFile(String filePath){
        try {
            FileReader reader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(reader);
            long lineCount = 0;
            while(bufferedReader.ready()){
                lineCount++;
                bufferedReader.readLine();
            }
            return lineCount;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
