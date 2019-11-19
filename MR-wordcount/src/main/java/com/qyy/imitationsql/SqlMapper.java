package com.qyy.imitationsql;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class SqlMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String m = "";

        //k2为学号，v2是其他信息
        //需要判断文件来源 截取字符串 进行join链接
        FileSplit file = (FileSplit)context.getInputSplit();
        String name = file.getPath().getName();

        String val = value.toString();

        //在user中截取最后一个字符串 在major中截取第一个字符串
        if(name.contains("user")){
            String[] split = value.toString().split(",");
            m = split[3];
        }

        if(name.contains("major")){
            String[] split = value.toString().split(",");
            m = split[0];
        }

        context.write(new Text(m),new Text(val));
    }
}
