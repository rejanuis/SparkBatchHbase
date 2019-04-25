package org.research.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HbaseConf {

    private String TablesInput;
    private String Hbasemaster;
    private String Zookeeperquorum;

    public HbaseConf(String TablesInput, String Hbasemaster, String Zookeeperquorum) {
        this.TablesInput = TablesInput;
        this.Hbasemaster = Hbasemaster;
        this.Zookeeperquorum = Zookeeperquorum;
    }

    private Configuration HbaseConfig() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", Hbasemaster);
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.zookeeper.quorum", Zookeeperquorum);
        configuration.set("hbase.client.keyvalue.maxsize", "0");
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "100000");
        configuration.set("mapred.output.dir", "/tmp");
        configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");

        return configuration;
    }

    public Job HbaseJob() throws IOException {
        Job job = Job.getInstance(HbaseConfig());
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TablesInput);
        job.setOutputFormatClass(TableOutputFormat.class);

        return job;
    }
}
