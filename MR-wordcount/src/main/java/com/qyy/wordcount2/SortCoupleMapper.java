package com.qyy.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class SortCoupleMapper extends Mapper<LongWritable, Text,Pair, IntWritable> {
    //设置k2,v2
    private static Pair k2 = new Pair();
    private static IntWritable v2 = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //设置计数器
        Counter counter = context.getCounter("map_input_hello_counter", "hello_count");

        if(value.toString().contains("hello"))
            counter.increment(1L);


        StringTokenizer tokenizer = new StringTokenizer(value.toString(),",");
        while (tokenizer.hasMoreTokens()){
            String[] split = tokenizer.nextToken().split("-");
            k2.setValue(split[0]);
            k2.setNum(Integer.parseInt(split[1]));
            context.write(k2,v2);
        }
    }

    /*public static void main(String[] args) {
        String str = "hello    2,hello    1,word  1";
        StringTokenizer tokenizer = new StringTokenizer(str, ",");
        System.out.println(Arrays.asList(tokenizer.nextToken().split("\t")));
    }*/
}
