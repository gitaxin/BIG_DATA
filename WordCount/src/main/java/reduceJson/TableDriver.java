package reduceJson;

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
 * @Date: Create in 15:36 2019/7/28
 */
public class TableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job j = Job.getInstance(conf);

        j.setJarByClass(TableDriver.class);
        j.setMapperClass(TableMapper.class);
        j.setReducerClass(TableReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TableBean.class);
        j.setOutputKeyClass(TableBean.class);
        j.setOutputValueClass(NullWritable.class);


        String inputPath = "E:\\test_input\\reducejson";
        String outputPath = "E:\\test_out\\" + System.currentTimeMillis();

        FileInputFormat.setInputPaths(j,new Path(inputPath));
        FileOutputFormat.setOutputPath(j,new Path(outputPath));

        j.waitForCompletion(true);

        }

}
