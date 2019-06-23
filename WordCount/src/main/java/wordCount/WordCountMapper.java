package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Axin
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.转换格式
        //ctrl+shift+enter \ ctrl + alt + v
        String line = value.toString();
        //2.切分数据
        String[] words = line.split(" ");

        //3.输出成<单词, 1>
        for (String word: words) {
            k.set(word);
            v.set(1);
            context.write(k, v);
        }
    }
}
