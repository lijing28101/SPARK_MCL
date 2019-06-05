package com.isu.gdcb.wurtelelab.spark;

import com.isu.gdcb.wurtelelab.encoder.CytoscapeGraphEncoder;
import com.isu.gdcb.wurtelelab.encoder.D3GraphEncoder;
import com.isu.gdcb.wurtelelab.encoder.GraphEncoder;
import com.isu.gdcb.wurtelelab.util.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by nishanth and Manhoi Hur on 4/12/17.
 */
public class SparkGraphBuilder implements Serializable{

    private String graphType            =   "";
    private int minimumClusterSize      =   0;
    private GraphEncoder graphEncoder   =   null;

    public SparkGraphBuilder(String graphType, int minimumClusterSize){

        this.graphType = graphType;
        this.minimumClusterSize = minimumClusterSize;
        if(graphType.equals("d3")){
            graphEncoder = new D3GraphEncoder();
        }else if(graphType.equals("cytoscape")){
            graphEncoder = new CytoscapeGraphEncoder();
        }
    }

    public void generateJSONData( JavaSparkContext javaSparkContext, String clusterOutputFile, String correlationMatrixFile, String sortedVertexFile, String probeToGeneMappingFile, String outputPath){

       final Map<String,Long> probeIndexMap = buildVertexIndexMap(javaSparkContext, sortedVertexFile);
       final Map<String, String> probeToGeneMap = buildProbeToGeneMap(javaSparkContext, probeToGeneMappingFile);
       //build vertex map with gene id instead of probe id
       final Map<Long, String> indexedGeneVertexMap = buildReverseGeneIndex(probeIndexMap, probeToGeneMap);
       if(indexedGeneVertexMap != null){
           JavaPairRDD<Long, Long> clusterVertexRDD = getClusterVertexRDD(javaSparkContext, clusterOutputFile);
           if(clusterVertexRDD != null){
               final Map<Long,Long> vertexClusterMap = clusterVertexRDD.collectAsMap();
               JavaRDD<String> clusterEdgesCSVRDD = getClusterEdgesRDD(javaSparkContext, correlationMatrixFile, probeIndexMap, vertexClusterMap);
               JavaRDD<String> clusterVertexCSVRDD = clusterVertexRDD.map(new Function<Tuple2<Long, Long>, String>() {
                   public String call(Tuple2<Long, Long> stringLongTuple2) throws Exception {
                       return graphEncoder.getVertexString(stringLongTuple2._1(),indexedGeneVertexMap.get(stringLongTuple2._1()), stringLongTuple2._2().intValue());
                   }
               });
               if(this.graphType.equals("d3")){
                   FileUtil.saveGraphInformation(clusterVertexCSVRDD, clusterEdgesCSVRDD, this.graphType, outputPath);
               }else if(this.graphType.equals("cytoscape")){
                   System.out.println("No header found for cytoscape");
               }
           }
       }
    }

    private Map<String,String> buildProbeToGeneMap( JavaSparkContext javaSparkContext, String probeToGeneMappingFile) {

        if(probeToGeneMappingFile != null && new File(probeToGeneMappingFile).isFile()){
            JavaRDD<String> probeToGeneRDD = javaSparkContext.textFile(probeToGeneMappingFile);
            JavaPairRDD<String, String> probeToGeneMap = probeToGeneRDD.mapToPair(new PairFunction<String, String, String>() {
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] fields = s.split(",");
                    if (fields != null && fields.length == 2) {
                        return new Tuple2<String, String>(fields[0], fields[1]);
                    }
                    return null;
                }
            });
            return probeToGeneMap.collectAsMap();
        }
        return null;
    }

    private JavaRDD<String> getClusterEdgesRDD( JavaSparkContext javaSparkContext, String correlationMatrixFile, final Map<String, Long> probeIndexMap, final Map<Long, Long> vertexClusterMap) {

        if(correlationMatrixFile != null && new File(correlationMatrixFile).isFile()){
            JavaRDD<String> correlationStringRDD = javaSparkContext.textFile(correlationMatrixFile);
            correlationStringRDD = javaSparkContext.parallelize(correlationStringRDD.collect(),31);

            JavaRDD<String> edgeCSVRDD = correlationStringRDD.flatMap(new FlatMapFunction<String, String>() {
                public Iterator<String> call(String s) throws Exception {
                    String[] fields = s.split(",");

                    if (fields.length > 0) {
                        String vertex   =   fields[0];
                        long startIndex =   probeIndexMap.get(vertex);
                        if(vertexClusterMap.get(startIndex) != null){
                            List<String> edgeList = new ArrayList<String>();
                            for (int i = 1; i < (startIndex+1); i++) {
                                double val = Double.parseDouble(fields[i]);
                                long endIndex =(long) (i - 1);
                                if (vertexClusterMap.get(endIndex) != null && val > 0) {
                                    edgeList.add(graphEncoder.getEdgeString(startIndex, endIndex, val));
                                }
                            }

                            return edgeList.iterator();
                        }
                        return new ArrayList<String>().iterator();
                    }
                    return null;
                }
            });

            edgeCSVRDD = edgeCSVRDD.filter(new Function<String, Boolean>() {
                public Boolean call(String s) throws Exception {
                    if(s != null && !s.trim().equals("")) return true;
                    return false;
                }
            }) ;

            return edgeCSVRDD;
        }
        return null;
    }

    private Map<Long,String> buildReverseGeneIndex(Map<String, Long> vertexIndexMap, Map<String, String> probeToGeneMap) {
        Map<Long, String> indexedVertexMap = new HashMap<Long, String>();
        for(String key: vertexIndexMap.keySet()){
            indexedVertexMap.put(vertexIndexMap.get(key), probeToGeneMap.get(key));
        }
        return indexedVertexMap;
    }

    private JavaPairRDD<Long, Long> getClusterVertexRDD( JavaSparkContext javaSparkContext, String clusterOutputFile) {

        if(clusterOutputFile != null && new File(clusterOutputFile).isFile()) {
            JavaRDD<String> clusterStringRDD = javaSparkContext.textFile(clusterOutputFile);
            JavaPairRDD<Long, Long> vertexClusterMap = clusterStringRDD.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
                public Iterator<Tuple2<Long, Long>> call(String s) throws Exception {
                    if (s != null && !s.trim().equals("")) {
                        s = s.replace("(", "").replace(")", "");
                        s = s.replace("[", "").replace("]", "");
                        String[] fields = s.split(",");
                        if (fields.length > minimumClusterSize) {
                            long clusterId = Long.parseLong(fields[0].trim());
                            List<Tuple2<Long, Long>> clusterMapList = new ArrayList<Tuple2<Long, Long>>();
                            for (int i = 1; i < fields.length; i++) {
                                long vertexId = Long.parseLong(fields[i].trim());
                                clusterMapList.add(new Tuple2<Long, Long>(vertexId, clusterId));
                            }
                            return clusterMapList.iterator();
                        }
                        return new ArrayList<Tuple2<Long, Long>>().iterator() ;
                    }
                    return null;
                }
            });
            vertexClusterMap = vertexClusterMap.filter(new Function<Tuple2<Long, Long>, Boolean>() {
                public Boolean call(Tuple2<Long, Long> longLongTuple2) throws Exception {
                    if(longLongTuple2 != null)
                        if(longLongTuple2._1 != null && longLongTuple2._2 != null)  return true;
                    return false;
                }
            });
            return vertexClusterMap;
        }
        return null;
    }

    private Map<String, Long> buildVertexIndexMap(JavaSparkContext javaSparkContext, String sortedVertexFile) {
        if(sortedVertexFile != null &&  new File(sortedVertexFile).isFile()){
            JavaRDD<String> vertexStringRDD = javaSparkContext.textFile(sortedVertexFile);

            JavaPairRDD<String, Long> vertexIndexRDD = vertexStringRDD.mapToPair(new PairFunction<String, String, Long>() {
                public Tuple2<String, Long> call(String s) throws Exception {
                    if (s != null && !s.trim().equals("")) {
                        s = s.replace("(", "").replace(")", "");
                        String[] fields = s.split(",");
                        if (fields != null && fields.length == 2) {
                            return new Tuple2<String, Long>(fields[1], Long.parseLong(fields[0]));
                        }
                    }
                    return null;
                }
            });
            return vertexIndexRDD.collectAsMap();
        }
        return null;
    }
}
