package com.qyy.imitationsql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;


/**
 * @author qyhsmx@outlook.com
 * @date
 */
public class JobMain  extends Configured implements Tool {
    /**
     * select * from user u left join major m on u.major = m.id
     * @param args
     */
    /*public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJobName("sqlJob");
        job.setJarByClass(JobMain.class);

        job.setMapperClass(SqlMapper.class);
        job.setReducerClass(SqlReducer.class);

        job.setInputFormatClass(FileInputFormat.class);
        job.setOutputFormatClass(FileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]),new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }*/
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        int res = ToolRunner.run(conf, new JobMain(), args);

        System.exit(res);

    }


    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "job-sql");
        job.setJarByClass(getClass());
        job.setMapperClass(SqlMapper.class);
        job.setReducerClass(SqlReducer.class);

//        job.setInputFormatClass(FileInputFormat.class);
//        job.setOutputFormatClass(FileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("/usr/sql/user.txt"),new Path("/usr/sql/major.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/usr/sql/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

        return 0;
    }
}
