package ETLLOG2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 20:53 2019/6/25
 */
public class LoggerDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        args = new String[]{"E:\\TEST\\web.txt","E:\\TEST\\out_web[2]"+System.currentTimeMillis()} ;
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //加载反射类
        job.setJarByClass(LoggerDriver.class);
        job.setMapperClass(LoggerMapper.class);

        //job.setReducerClass();

        /*map的输出类型*/
        //job.setMapOutputKeyClass();
        //job.setMapOutputValueClass();
        //reduce的输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.waitForCompletion(true);


    }
}
