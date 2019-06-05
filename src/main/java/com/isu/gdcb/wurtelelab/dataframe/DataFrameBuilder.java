package com.isu.gdcb.wurtelelab.dataframe;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nishanth on 3/2/17.
 */
public class DataFrameBuilder {
    private List<String> colNames;
    private JavaRDD<Row> rowJavaRDD;
    private SparkSession sparkSession;
    private StructType schema;

    public DataFrameBuilder(List<String> colNames, JavaRDD<Row> rowJavaRDD){
        this.colNames = colNames;
        this.rowJavaRDD = rowJavaRDD;
        initializeSparkSession();
        buildColumnStructTypes();
    }

    private void buildColumnStructTypes() {
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : colNames) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        this.schema = DataTypes.createStructType(fields);
    }

    public Dataset<Row> buildDataFrame(){
        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);
        return peopleDataFrame;
    }

    private void initializeSparkSession() {
        sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
    }
}
