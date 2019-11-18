package com.qyy.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class WordCountJob {
    public static void main(String[] args) throws Exception{
        JobConf jobConf = new JobConf(WordCountJob.class);
        jobConf.setJobName("wordcount");

        //map
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setMapperClass(WordCountMapper.class);
        FileInputFormat.setInputPaths(jobConf,new Path(args[0]));


        //partition
        jobConf.setPartitionerClass(WordLengthPartitioner.class);
        jobConf.setNumReduceTasks(2);

        //combiner
        jobConf.setCombinerClass(WordCountReduce.class);

        //reduce
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.setReducerClass(WordCountReduce.class);
        FileOutputFormat.setOutputPath(jobConf,new Path(args[1]));

        JobClient.runJob(jobConf);
    }
}
