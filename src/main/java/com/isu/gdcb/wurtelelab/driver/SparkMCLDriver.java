package com.isu.gdcb.wurtelelab.driver;

import com.isu.gdcb.wurtelelab.spark.SparkGraphBuilder;
import com.isu.gdcb.wurtelelab.spark.SparkMCL;
import com.isu.gdcb.wurtelelab.spark.SparkCorrelation;
import com.isu.gdcb.wurtelelab.util.FileUtil;
import com.isu.gdcb.wurtelelab.util.StringUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.*;

/**
 * Created by nishanth and Manhoi Hur on 4/12/17.
 */
public class SparkMCLDriver {

    private static final String SPARK_MODE                  = "local[*]";
    private static final String CORRELATION_OUTPUT_FOLDER   =   "correlation_output";
    private static final String VERTEX_OUTPUT_FOLDER    =   "vertices_output";

    private final int DEFAULT_NUM_PARTITIONS            =   31;
    private final String DEFAULT_CORRELATION_TYPE       =   "pearson";
    private final double DEFAULT_ADJACENCY_THRESHOLD    =   0.7;

    private final double DEFAULT_INFLATION_RATE         =   2.0;
    private final int DEFAULT_EXPANSION_RATE            =   2;
    private final int DEFAULT_NUM_ITERATIONS            =   10;
    private final double DEFAULT_EPSILON                =   0.02;
    private final double DEFAULT_SELF_LOOP_WEIGHT       =   1;
    private final String DEFAULT_GRAPH_TYPE             =   "undirected";

    public void computeCorrelation(String inputFilePath, String outputPath, double adjacencyThreshold, String correlationType, int numPartitions, int corr_method, int isLog2){
        if(correlationType.equals("pearson")){
            File inputFile = new File(inputFilePath);
            if(inputFile.isFile()){
                SparkConf sparkConf = new SparkConf().setAppName(SparkCorrelation.class.getName()).setMaster(SPARK_MODE);
                JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

                SparkCorrelation sPearsonCorr = new SparkCorrelation(numPartitions, adjacencyThreshold, corr_method, isLog2);
                sPearsonCorr.computeCorrelation(sparkConf, javaSparkContext, inputFilePath, outputPath+File.separator+CORRELATION_OUTPUT_FOLDER, outputPath+ File.separator+VERTEX_OUTPUT_FOLDER);
                javaSparkContext.stop();
            }else{
                System.out.println("No File Found at "+inputFilePath);
            }
        }else{
            System.out.println("No Correlation Method found for: "+correlationType);
        }
    }

    public void computeCorrelation(String inputFilePath, String outputPath, double adjacencyThreshold, int corr_method, int isLog2){
         this.computeCorrelation(inputFilePath, outputPath, adjacencyThreshold, this.DEFAULT_CORRELATION_TYPE, this.DEFAULT_NUM_PARTITIONS, corr_method, isLog2);
    }

    public void executeMCL(String correlationMatrixFilePath, String sortedVertexFilePath, String outputPath, double inflationRate, int expansionRate, double epsilon, double selfLoopWeight, int numIterations, String graphType){
        File matrixFile = new File(correlationMatrixFilePath);
        if(matrixFile.isFile()){
            File vertexFile = new File(sortedVertexFilePath);
            if(vertexFile != null && vertexFile.isFile()){
                SparkConf sparkConf = new SparkConf().setAppName(SparkMCL.class.getName()).setMaster(SPARK_MODE);
                JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
                SparkMCL sparkMCL = new SparkMCL(inflationRate, expansionRate, epsilon, numIterations, selfLoopWeight, graphType);
                sparkMCL.runMCL(javaSparkContext, correlationMatrixFilePath, sortedVertexFilePath, outputPath );
                javaSparkContext.stop();
            }else{
                System.out.println("No File Found at "+vertexFile);
            }
        }else{
            System.out.println("No File Found at "+correlationMatrixFilePath);
        }
    }

    public void executeMCL(String correlationMatrixFilePath, String sortedVertexFilePath, String outputPath, double inflationRate, int numIterations){
        this.executeMCL(correlationMatrixFilePath, sortedVertexFilePath, outputPath, inflationRate, this.DEFAULT_EXPANSION_RATE, this.DEFAULT_EPSILON, this.DEFAULT_SELF_LOOP_WEIGHT, numIterations, this.DEFAULT_GRAPH_TYPE);
    }

    public void generateGraphJSON(int minimumClusterSize, String clusterOutputFile, String correlationMatrixFile, String sortedVertexFile, String probeToGeneMappingFile, String outputPath){
        if(clusterOutputFile != null && new File(clusterOutputFile).isFile()){
            if(correlationMatrixFile != null && new File(correlationMatrixFile).isFile()){
                if(sortedVertexFile != null && new File(sortedVertexFile).isFile()){
                    if(probeToGeneMappingFile != null && new File(probeToGeneMappingFile).isFile()) {
                        SparkConf sparkConf = new SparkConf().setAppName(SparkGraphBuilder.class.getName()).setMaster(SPARK_MODE);
                        sparkConf.set("spark.driver.maxResultSize","32g");
                        JavaSparkContext javaSparkContext   =   new JavaSparkContext(sparkConf);
                        SparkGraphBuilder sparkGraphBuilder =   new SparkGraphBuilder("d3", minimumClusterSize);
                        sparkGraphBuilder.generateJSONData(javaSparkContext, clusterOutputFile, correlationMatrixFile, sortedVertexFile, probeToGeneMappingFile, outputPath);
                        javaSparkContext.stop();
                    }
                    else{
                        System.out.println("No Gene Vertex Mapping File found at " + probeToGeneMappingFile);
                    }
                }else{
                    System.out.println("No Sorted Vertex File found at " + sortedVertexFile);
                }
            }
            else{
                System.out.println("No Correlation File found at " + correlationMatrixFile);
            }
        }else{
            System.out.println("No Cluster file found at " + clusterOutputFile);
        }
    }

    public static void main(String[] args){
        SparkMCLDriver driver = new SparkMCLDriver();
        String base_dir                 =   "D:\\SparkMCLProjects\\mhhur\\MCL_EXP\\";
        String experiment_base_dir      =   base_dir    +   "output_files\\jingli";
        String data_file_path           =   base_dir    +   "input_file\\jingli\\anno\\anno_logcpm_SRR5507444.csv";
        String experiment_output_dir    =   experiment_base_dir;
        String probe_gene_mapping_file  =   base_dir    +   "mapping_file\\jingli\\JL_2018-11-12_annomap.csv";
        String mcl_output_dir_name      =   "mcl_output";
        String graph_output_dir_name    =   "graph_output";

        System.out.println("*************Max size:");
        int limited_params = 6; // don't change it
        // first: 0 is pearson and 1 is spearman
        // second: threshold for MCL
        // others is for MCL
        // 6th: to enable log2, you should set 1 // added by mhhur
        Object[] params = {0, 0.6, 100, 2, 2, 0};
        int i = 0;
        try {
            // open file
            FileWriter writer = new FileWriter(experiment_base_dir +   "summary.csv");
            writer.write("corr_method\tcorrelation_value\tnum_iterations\tinflation_rate\tmin_cluster_size\tcorrelation_time(min)\tmcl_time(min)\tgraph_output_time(min)\tnum_clusters\tnum_vertices\tnum_edges\n");
            while(i != ((params.length/limited_params)*limited_params)){
                String experiment_folder_name   =   StringUtil.buildFolderNameFromParams(
                                                            params[i].toString()
                                                        ,   params[i+1].toString()
                                                        ,   params[i+2].toString()
                                                        ,   params[i+3].toString());

                experiment_output_dir   = experiment_output_dir+File.separator+experiment_folder_name;

                int corr_method             =   Integer.parseInt(params[i++].toString());
                double correlationThreshold =   Double.parseDouble(params[i++].toString());
                int numIterations           =   Integer.parseInt(params[i++].toString());
                double inflationRate        =   Double.parseDouble(params[i++].toString());
                int minClusterSize          =   Integer.parseInt(params[i++].toString());
                int isLog2                  =   Integer.parseInt(params[i++].toString());

                // Compute correlation /////////////////////////////////////
                long corr_start_time        = System.currentTimeMillis();
                driver.computeCorrelation(data_file_path, experiment_output_dir, correlationThreshold, corr_method, isLog2);

                String correlation_file_path    =   FileUtil.getPartitionFilePath(experiment_output_dir+File.separator+CORRELATION_OUTPUT_FOLDER+File.separator+"r_value");
                String vertexFilePath           =   FileUtil.getPartitionFilePath(experiment_output_dir+File.separator+VERTEX_OUTPUT_FOLDER);

                while(correlation_file_path == null || vertexFilePath == null){
                    correlation_file_path   =  FileUtil.getPartitionFilePath(experiment_output_dir+File.separator+CORRELATION_OUTPUT_FOLDER);
                    vertexFilePath          =   FileUtil.getPartitionFilePath(experiment_output_dir+File.separator+VERTEX_OUTPUT_FOLDER);
                }
                long corr_stop_time = System.currentTimeMillis();
                ////////////////////////////////////////////////////////////

                // Running MCL /////////////////////////////////////////////
                long mcl_start_time = System.currentTimeMillis();
                driver.executeMCL(correlation_file_path, vertexFilePath, experiment_output_dir+File.separator+mcl_output_dir_name , inflationRate, numIterations);

                String cluster_file_path = FileUtil.getPartitionFilePath(experiment_output_dir+File.separator+mcl_output_dir_name);
                while(cluster_file_path == null){
                    cluster_file_path = FileUtil.getPartitionFilePath(experiment_output_dir+File.separator+mcl_output_dir_name);
                }
                long mcl_stop_time  =   System.currentTimeMillis();
                ////////////////////////////////////////////////////////////////////////

                // Get number of clusters
                long numClusters    =   FileUtil.countLinesInFile(cluster_file_path);

                // Create edge and vertex files /////////////////////////////
                long graph_start_time       =   System.currentTimeMillis();
                String edge_output_path     =   experiment_output_dir+File.separator+graph_output_dir_name+File.separator + "edge_output";
                String vertex_output_path   =   experiment_output_dir+File.separator+graph_output_dir_name+File.separator + "vertex_output";

                driver.generateGraphJSON(minClusterSize
                                        , cluster_file_path
                                        , correlation_file_path
                                        , vertexFilePath
                                        , probe_gene_mapping_file
                                        , experiment_output_dir+File.separator+graph_output_dir_name);

                String edge_output_file     =   FileUtil.getPartitionFilePath(edge_output_path);
                String vertex_output_file   =   FileUtil.getPartitionFilePath(vertex_output_path);

                while(edge_output_file == null || vertex_output_file == null ){
                    edge_output_file    =   FileUtil.getPartitionFilePath(edge_output_path);
                    vertex_output_file  =   FileUtil.getPartitionFilePath(vertex_output_path);
                }
                long graph_stop_time = System.currentTimeMillis();
                /////////////////////////////////////////////////////////////

                // Create edges
                long numVertices    =   FileUtil.countLinesInFile(vertex_output_file);
                long numEdges       =   FileUtil.countLinesInFile(edge_output_file);
                writer.write(corr_method                        +   "\t"
                            +   correlationThreshold                +   "\t"
                            +   numIterations                       +   "\t"
                            +   inflationRate                       +   "\t"
                            +   minClusterSize                      +   "\t"
                            +   Math.round((((corr_stop_time-corr_start_time)/1000D)/60D) * 1000)/1000D    +   "\t"
                            +   Math.round((((mcl_stop_time-mcl_start_time)/1000D)/60D)  * 1000)/1000D     +   "\t"
                            +   Math.round((((graph_stop_time-graph_start_time)/1000D)/60D) * 1000)/1000D  +   "\t"
                            +   numClusters                         +   "\t"
                            +   numVertices                         +   "\t"
                            +   numEdges                            +   "\n");

                // reset output dir.
                experiment_output_dir   =   experiment_base_dir;
                System.gc();
            }
            // close file
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}