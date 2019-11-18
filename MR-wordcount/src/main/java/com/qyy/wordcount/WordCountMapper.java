package com.qyy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class WordCountMapper implements Mapper<LongWritable, Text,Text, IntWritable>{
    //设置v1和k1
    private static IntWritable v1 = new IntWritable(1);
    private static Text k1 = new Text();


    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        //先分割字符
        String string = text.toString();
        StringTokenizer tokenizer = new StringTokenizer(string,",");
        while (tokenizer.hasMoreTokens()){
            k1.set(tokenizer.nextToken());
            outputCollector.collect(k1,v1);
        }
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
