package wordCount;

        import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.IOException;

public class WordCountDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        for (String string : args) {
            System.out.println(string);
        }
        //输入数据的windows路径 ， 输出的windows路径
        //args = new String[]{"E:\\教程\\大数据\\课件\\0327-MapReduce编程基础\\hello.txt","E:\\ASDFA "};

        //1.获取配置信息
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //反射三个类
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //map输出的 KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce输出的K,V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //数据的输入和输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        /* 提交job， waitForCompletion包含job.submit() */
        job.waitForCompletion(true);


    }

}
