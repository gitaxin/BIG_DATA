package wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author:jfkdsljfkdsl;
 * @Date: <code>dhfsafkdjsf</code>;
 *
 *
 * MapReduce编程主要组件
 * InputFormat类：分割成多个splits和每行怎么解析。
 * Mapper类：对输入的每对<key,value>生成中间结果。
 * Combiner类：在map端，对相同的key进行合并。
 * Partitioner类：在shuffle过程中，将按照key值将中间结果分为R份，每一份都由一个reduce去完成。
 * Reducer类：对所有的map中间结果，进行合并。
 * OutputFormat类：负责输出结果格式。
 * 编程框架如下：
 */
public class WordCountDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        for (String string : args) {
            System.out.println(string);
        }
        //输入数据的windows路径 ， 输出的windows路径
        args = new String[]{"E:\\TEST\\mu","E:\\TEST\\out_"+System.currentTimeMillis()};

        //1.获取配置信息
        Configuration conf = new Configuration();

        //开启map输出压缩
        conf.setBoolean("mapreduce.map.output.compress",true);
        //设置map输出压缩方式
        conf.setClass("mapreduce.map.output.compress",BZip2Codec.class,CompressionCodec.class);

        Job job = Job.getInstance(conf);

        //反射三个类在
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //maptask数据预合并， maptask的局部处理 。 Map中往Reduce移动时，先在Map中进行一次预合并，再输出给Reduce阶段。
        //job.setCombinerClass(WordCountReducer.class);


        //设置分区,不设置的话默认使用HashPartitioner
//        job.setPartitionerClass(WordCountPartitioner.class);
//        job.setNumReduceTasks(3);//此处是几就分几个区，但如果此数小于分区数(代码中预计返回的分区数量)并且大于1，会报错


        //切片优化：针对于小文件
        //如果不设置InputFormat，它默认用的是TextInputFormat.class, 1个文件1个切片
        //设置后就不是按文件个数来分maptask了
        //优先满足最小切片大小,不超过最大切片大小：举例:0.5m+1m+0.3m+5m=2m + 4.8m=2m + 4m + 0.8m
        //参考资料:https://blog.csdn.net/wwwzydcom/article/details/83962836
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineFileInputFormat.setMaxInputSplitSize(job,3*1024*1024);
        CombineFileInputFormat.setMinInputSplitSize(job,2*1024*1024);

        //map输出的 KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce输出的K,V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //开启reduce端输出压缩
        FileOutputFormat.setCompressOutput(job,true);
        //设置reduce端输出压缩格式
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);


        //数据的输入和输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));



        /* 提交job， waitForCompletion包含job.submit() ：提交给yarn去运行*/
        job.waitForCompletion(true);


    }

}
