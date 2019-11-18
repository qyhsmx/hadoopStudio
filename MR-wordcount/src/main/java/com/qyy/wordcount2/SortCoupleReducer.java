package com.qyy.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class SortCoupleReducer extends Reducer<Pair,IntWritable,Pair,IntWritable> {

    //java.lang.Iterable
    //java.util.Iterator
    @Override
    protected void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger sum = new AtomicInteger();
        values.forEach(val->{
            sum.addAndGet(val.get());
        });
        context.write(key,new IntWritable(sum.intValue()));
    }
}
