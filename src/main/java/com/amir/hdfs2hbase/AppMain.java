package com.amir.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * hdfs 中的 数据通过MR计算写入HBASE
 */
public class AppMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 本地运行
        conf.set("mapreduce.framework.name", "local");
        // 设置hbase运行的zk集群
        conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
        // 设置环境变量
        System.setProperty("HADOOP_USER_NAME", "root");
        conf.set("mapreduce.cluster.local.dir","/Users/maningyu/workspace/javaprojects/hbaseapi_demo/src/main/resources");

        // 创建job对象
        Job job = Job.getInstance(conf, "hdfs2hbase_wordcount");
        job.setJarByClass(AppMain.class);

        // 指定输入文件路径
        FileInputFormat.addInputPath(job, new Path("/usr/local/hello.txt"));

        // 指定mapper
        job.setMapperClass(Hdfs2HbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 输出到hbase
        TableMapReduceUtil.initTableReducerJob(
                "wordcount", // 指定hbase 的表名
                Hdfs2HbaseReducer.class, // 指定hbase的reducer
                job,
                null,null,null,null,
                false);

        // 提交作业
        job.waitForCompletion(true);
    }
}
