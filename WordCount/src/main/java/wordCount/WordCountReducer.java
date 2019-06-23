package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Axin
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //初始化次数
        int count = 0;

        //汇总个数
        for (IntWritable value : values) {
            count += value.get();
        }

        v.set(count);
        //输出总次数
        context.write(key, v);

    }
}
