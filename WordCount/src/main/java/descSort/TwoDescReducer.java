package descSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 23:43 2019/8/1
 */
public class TwoDescReducer extends Reducer<Text,Text,Text,Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value).append("\t");
        }

        context.write(key,new Text(sb.toString()));

    }
}
