package outputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:23 2019/8/2
 */
public class FilterMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        k.set(line);
        context.write(k,NullWritable.get());
    }
}
