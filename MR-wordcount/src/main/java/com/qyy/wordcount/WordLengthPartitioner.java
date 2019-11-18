package com.qyy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class WordLengthPartitioner implements Partitioner<Text, IntWritable> {

    public int getPartition(Text text, IntWritable intWritable, int i) {
        int length = text.toString().length();
        if(length>3){
            return 0;
        }else{
            return 1;
        }
    }

    public void configure(JobConf jobConf) {

    }
}
