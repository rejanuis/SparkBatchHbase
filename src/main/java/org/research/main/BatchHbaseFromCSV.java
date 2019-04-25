package org.research.main;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.research.config.HbaseConf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;


public class BatchHbaseFromCSV {
    public static void main(String[] args) throws IOException {
        // Initialize global variable
//        String tablesname = args[0];
//        String zkquorum = args[1];
//        String hbasemaster = args[2];
//        String pathfilecsv = args[3];

        // get paramtere from livy
        String tablesname = "winevariety";
        String zkquorum = "192.168.114.84,192.168.114.171,192.168.114.172";
        String hbasemaster = "192.168.114.84";
        String pathfilecsv = "file:///home/ebdesk/IdeaProjects/SparkBatchHbase/winemag-data_first150k2.csv";

        // Initialize spark
        HbaseConf confs = new HbaseConf(tablesname, hbasemaster, zkquorum);
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);
        SparkSession sparks = SparkSession.builder().appName("BatchHbaseFromCSV").master("local[2]").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparks.sparkContext());

        //read file csv using spark and convert into Java RDD
        JavaRDD<Row> data = sparks.read().option("header",true).option("delimiter", "|").csv(pathfilecsv).toJavaRDD();
//          Dataset<Row> df = sparks.read().option("header",true).option("delimiter", ",").csv(pathfilecsv);
//          df.show(1000);

        //  transformation Spark
        JavaPairRDD<ImmutableBytesWritable, Put> pairRDD = data.filter(s -> {
            //Filter to get not null data
            Boolean state = false;
            if (s.get(10) != null && s.get(9) != null && s.get(5) != null) {
                state = true;
            }
            return state;
        }).mapToPair( s -> {
            Put put = new Put(Bytes.toBytes(s.get(10).toString().replace(" ", "_")));

            // Add HBase column family, column qualifier, and column value.
            // price as column family, 9 is column winery value as column qualifier, and 5 is price value as column value.
            put.addColumn(Bytes.toBytes("price"), Bytes.toBytes(s.get(9).toString().replace(" ", "_")), Bytes.toBytes(s.get(5).toString()));

            return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
        });

        // spark action save to hbase
        pairRDD.saveAsNewAPIHadoopDataset(confs.HbaseJob().getConfiguration());

        // stop spark
        sparks.stop();
    }
}
