package com.qyy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class WordCountReduce implements Reducer<Text, IntWritable,Text,IntWritable> {
    //设置v3
    private static IntWritable v3 = new IntWritable();
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while (iterator.hasNext()){
            sum += iterator.next().get();
        }
        v3.set(sum);
        outputCollector.collect(text,v3);
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
