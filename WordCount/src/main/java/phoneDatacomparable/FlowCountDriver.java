package phoneDatacomparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job j = Job.getInstance(conf);

        j.setJarByClass(FlowCountDriver.class);
        j.setMapperClass(FlowCountMapper.class);
        j.setReducerClass(FlowCountReducer.class);

        j.setMapOutputKeyClass(FlowBean.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FlowBean.class);


        String inputPath = "E:\\test_out\\1564305398257\\part-r-00000";
        String outputPath = "E:\\test_out\\" + System.currentTimeMillis();

        FileInputFormat.setInputPaths(j,new Path(inputPath));
        FileOutputFormat.setOutputPath(j,new Path(outputPath));

        j.waitForCompletion(true);

        }

}
