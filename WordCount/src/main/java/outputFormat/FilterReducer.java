package outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:29 2019/8/2
 */
public class FilterReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String line = key.toString();

        line += "\r\n";//加回车换行

        context.write(new Text(line),NullWritable.get());

    }
}
