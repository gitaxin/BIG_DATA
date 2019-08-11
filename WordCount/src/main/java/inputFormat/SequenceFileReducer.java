package inputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:01 2019/8/2
 */
public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable>{

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {


        context.write(key,values.iterator().next());

    }
}
