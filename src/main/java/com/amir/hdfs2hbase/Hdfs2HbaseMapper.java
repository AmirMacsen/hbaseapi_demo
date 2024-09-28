package com.amir.hdfs2hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Hdfs2HbaseMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 定义输出的key和value
    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 将读取到的内容按照空格进行拆分
        String[] words = value.toString().split(" ");
        // 遍历words
        for (String word : words) {
            // 输出
            outKey.set(word);
            // 输出到圆形缓冲区
            context.write(outKey, outValue);
        }
    }
}
