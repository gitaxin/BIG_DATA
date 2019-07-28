package groupingComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 18:02 2019/7/28
 */
public class OrderDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {



        Configuration conf = new Configuration();
        Job j = Job.getInstance(conf);

        j.setJarByClass(OrderDriver.class);
        j.setMapperClass(OrderMapper.class);
        j.setReducerClass(OrderReducer.class);

        j.setMapOutputKeyClass(OrderBean.class);
        j.setMapOutputValueClass(NullWritable.class);
        j.setOutputKeyClass(OrderBean.class);
        j.setOutputValueClass(NullWritable.class);


        j.setGroupingComparatorClass(OrderGroupingComparator.class);

        j.setPartitionerClass(OrderPartitioner.class);
        j.setNumReduceTasks(3);

        String inputPath = "E:\\test_input\\GroupingComparator.txt";
        String outputPath = "E:\\test_out\\" + System.currentTimeMillis();

        FileInputFormat.setInputPaths(j,new Path(inputPath));
        FileOutputFormat.setOutputPath(j,new Path(outputPath));

        j.waitForCompletion(true);

    }
}
