package mapJoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:11 2019/7/29
 */
public class DistributedCacheDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(DistributedCacheDriver.class);
        job.setMapperClass(DistributedCacheMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);



        String inputPath = "E:\\test_input\\order.txt";
        String outputPath = "E:\\test_out\\" + System.currentTimeMillis();

        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));


        job.addCacheFile(new URI("file:///E://test_input//pd.txt"));
        job.setNumReduceTasks(0);


        job.waitForCompletion(true);


    }
}
