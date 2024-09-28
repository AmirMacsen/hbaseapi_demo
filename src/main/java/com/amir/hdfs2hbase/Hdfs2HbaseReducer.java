package com.amir.hdfs2hbase;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/***
 * TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>
 *     泛型参数写法，需与Mapper输出的key、value类型一致
 *     继承TableReducer，需实现reduce方法
 */
public class Hdfs2HbaseReducer extends TableReducer<Text, IntWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Mutation>.Context context) throws IOException, InterruptedException {
        // 定义变量，表示当前单词出现的次数
        int sum = 0;
        // 遍历values，求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 创建put对象, 以单词作为rowkey，列族为cf，列名为count，值为sum
        // 以下两种方式都可以，根据需要选择不同的方法
//        Put Put = new Put(key.toString().getBytes());
        Put put = new Put(Bytes.toBytes(key.toString()));
        // 为put指定列族
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(sum));
        // 输出
        context.write(key, put);
    }
}
