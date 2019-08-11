package descSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:28 2019/8/1
 */
public class OneDescMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    String name;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       FileSplit split = (FileSplit)context.getInputSplit();
       name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        for (String field : fields) {
            context.write(new Text(field + "--" + name),new IntWritable(1));
        }


    }
}
