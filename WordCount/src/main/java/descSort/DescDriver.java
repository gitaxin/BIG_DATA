package descSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:46 2019/8/1
 */
public class DescDriver {

    public static void main(String[] args) throws IOException {

        long time = System.currentTimeMillis();

        args = new String[]{"E:\\test_input\\desc","E:\\test_out\\" + time,"E:\\test_out\\" + System.currentTimeMillis()+1};
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);


        job.setMapperClass(OneDescMapper.class);
        job.setReducerClass(OneDescReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        Job job2 = Job.getInstance(conf);

        job2.setMapperClass(TwoDescmapper.class);
        job2.setReducerClass(TwoDescReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));

        JobControl control = new JobControl("axin");
        ControlledJob ajob = new ControlledJob(job.getConfiguration());
        ControlledJob bjob = new ControlledJob(job2.getConfiguration());
        bjob.addDependingJob(ajob);

        control.addJob(ajob);
        control.addJob(bjob);
        Thread thread = new Thread(control);
        thread.start();



    }
}
