package friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:15 2019/8/1
 */
public class TwoShareFriendsReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text friend : values) {
            sb.append(friend).append(" ");

        }
        context.write(key,new Text(sb.toString()));
    }
}
