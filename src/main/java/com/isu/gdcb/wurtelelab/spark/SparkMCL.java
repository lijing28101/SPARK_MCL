
         package com.isu.gdcb.wurtelelab.spark;

         import com.isu.gdcb.wurtelelab.util.FileUtil;
         import org.apache.spark.api.java.JavaPairRDD;
         import org.apache.spark.api.java.JavaRDD;
         import org.apache.spark.api.java.JavaSparkContext;
         import org.apache.spark.api.java.function.*;
         import org.apache.spark.graphx.Edge;
         import org.apache.spark.graphx.Graph;
         import org.apache.spark.mllib.clustering.Assignment;
         import org.apache.spark.mllib.clustering.MCL;
         import org.apache.spark.mllib.clustering.MCLModel;
         import org.apache.spark.sql.*;
         import org.apache.spark.storage.StorageLevel;
         import scala.Tuple2;
         import scala.reflect.ClassTag$;

         import java.io.File;
         import java.io.Serializable;
         import java.util.*;

/**
 * Created by nishanth on 3/6/17.
 */
public class SparkMCL implements Serializable{

    private static final String PSEUDO_DISTRIBUTED_SPARK_MODE = "local[*]";
    private final Map<String, Long> vertexIndexMap = new HashMap<String, Long>();
    private double inflationRate = 2;
    private int expansionRate = 2;
    private double epsilon = 0.0;
    private int numIterations = 100;
    private double selfLoopWeight = 1.0;
    private String graphType = "undirected";

    public SparkMCL(double inflationRate, int expansionRate, double epsilon, int numIterations, double selfLoopWeight, String graphType){
        this.inflationRate = inflationRate;
        this.expansionRate = expansionRate;
        this.epsilon = epsilon;
        this.numIterations = numIterations;
        this.selfLoopWeight = selfLoopWeight;
        this.graphType = graphType;
    }

    private JavaRDD<Tuple2<Object, Object>> getVertexRDD(JavaSparkContext javaSparkContext, String sortedVertexFile){

        if(sortedVertexFile != null && new File(sortedVertexFile).isFile()) {
            JavaRDD<String> indexedVertexStringRDD = javaSparkContext.textFile(sortedVertexFile);
            JavaRDD<Tuple2<Object, Object>> vertexRDD = indexedVertexStringRDD.map(new Function<String, Tuple2<Object, Object>>() {
                public Tuple2<Object, Object> call(String s) throws Exception {
                    if (s != null && !s.trim().equals("")) {
                        s = s.replace("(", "").replace(")", "");
                        String[] fields = s.split(",");
                        if (fields != null && fields.length == 2) {
                            return new Tuple2<Object, Object>(Long.parseLong(fields[0]), fields[1]);
                        }
                    }
                    return null;
                }
            });
            List<Tuple2<Object,Object>> verticesList = vertexRDD.collect();
            for(Tuple2<Object,Object> vertex: verticesList){
                vertexIndexMap.put(vertex._2().toString(), Long.parseLong(vertex._1().toString()));
            }
            return vertexRDD;
        }
        return null;
    }


    private JavaRDD<String> getCorrelationMartixAsString(JavaSparkContext javaSparkContext, String correlationMatrixFile){
        if(correlationMatrixFile != null && new File(correlationMatrixFile).isFile()) {
            JavaRDD<String> csvRDD = javaSparkContext.textFile(correlationMatrixFile);
            return csvRDD;
        }
        return null;
    }
    private JavaRDD<Edge<Object>> getEdgeRDD(JavaSparkContext javaSparkContext, String correlationMatrixFile){

        JavaRDD<String> csvRDD = getCorrelationMartixAsString(javaSparkContext, correlationMatrixFile);
        if(csvRDD != null) {
            JavaPairRDD<Long, List<Edge<Object>>> vertexEdgeRDD = csvRDD.mapToPair(new PairFunction<String, Long, List<Edge<Object>>>() {
                public Tuple2<Long, List<Edge<Object>>> call(String s) throws Exception {
                    String[] fields = s.split(",");
                    if(fields.length > 0){
                        long vertexId = vertexIndexMap.get(fields[0]);
                        List<Edge<Object>> edges = new ArrayList<Edge<Object>>();
                        for(int i = 1; i < (vertexId + 1 ); i++){
                            double val = Double.parseDouble(fields[i]);
                            if(val != 0) edges.add(new Edge<Object>(vertexId, i-1, 1.0));
                        }
                        return new Tuple2<Long, List<Edge<Object>>>(vertexId, edges);
                    }
                    return null;
                }
            });
            vertexEdgeRDD = vertexEdgeRDD.filter(new Function<Tuple2<Long, List<Edge<Object>>>, Boolean>() {
                public Boolean call(Tuple2<Long, List<Edge<Object>>> longListTuple2) throws Exception {
                    if(longListTuple2 == null || longListTuple2._2() == null || longListTuple2._2().size() < 1) return false;
                    return true;
                }
            });

            JavaRDD<Edge<Object>> edgeRDD = vertexEdgeRDD.values().flatMap(new FlatMapFunction<List<Edge<Object>>, Edge<Object>>() {

                public Iterator<Edge<Object>> call(List<Edge<Object>> edges) throws Exception {
                    return edges.iterator();
                }
            });
            return edgeRDD;
        }
        return null;
    }

    public void runMCL(JavaSparkContext javaSparkContext, String correlationMatrixFile, String sortedVertexFile, String mclOutputPath){

        //read verticesFilePath and buid vertex list
        JavaRDD<Tuple2<Object, Object>> vertexRDD = getVertexRDD(javaSparkContext, sortedVertexFile);

        JavaRDD<Edge<Object>> edgeRDD = getEdgeRDD(javaSparkContext, correlationMatrixFile);
        if(vertexRDD != null && edgeRDD != null) {

            scala.reflect.ClassTag<Object> classTag = ClassTag$.MODULE$.apply(Object.class);

            Graph<Object, Object> graph = Graph.apply(vertexRDD.rdd(), edgeRDD.rdd(), 0L, StorageLevel.MEMORY_AND_DISK(), StorageLevel.MEMORY_AND_DISK(), classTag, classTag);
            MCLModel mclModel =  MCL.train(graph, this.expansionRate, this.inflationRate, this.epsilon, this.numIterations, this.selfLoopWeight, this.graphType);
            Dataset<Assignment> assignments = mclModel.assignments();
            assignments.show();

            JavaRDD<Assignment> assignmentJavaRDD = assignments.toJavaRDD();
            JavaPairRDD<Long, Long> mclOutputRDD = assignmentJavaRDD.mapToPair(new PairFunction<Assignment, Long, Long>() {
                public Tuple2<Long, Long> call(Assignment assignment) throws Exception {
                    return new Tuple2<Long, Long>(assignment.cluster(), assignment.id());
                }
            });

            JavaPairRDD<Long, Iterable<Long>> clusterOutputRDD = mclOutputRDD.groupByKey();
            FileUtil.saveClusterRDD(clusterOutputRDD, mclOutputPath);
        }
    }
}