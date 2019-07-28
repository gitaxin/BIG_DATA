package Phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:31 2019/7/27
 */
public class PhoneMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        line = line.replaceAll(" ", "");
        text.set(line);
        context.write(text,NullWritable.get());

    }
}
