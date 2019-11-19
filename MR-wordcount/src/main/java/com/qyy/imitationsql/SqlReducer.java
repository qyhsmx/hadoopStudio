package com.qyy.imitationsql;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class SqlReducer extends Reducer<Text,Text,Text,Text> {

    /**
     * k2 103
     * v2 <val1,val2>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //key都是专业编码 values是学生信息加专业信息
        StringBuilder buf = new StringBuilder();

        for (Text value : values) {
            buf.append(value.toString()+"\t");
        }

        context.write(key,new Text(buf.toString()));


    }
}
