package friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description: 共同好友
 * @Author: Axin
 * @Date: Create in 21:44 2019/8/1
 */
public class OneShareFriendsMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split(":");
        String person = fields[0];
        String[] friends = fields[1].split(",");
        for (String friend : friends) {
            context.write(new Text(friend),new Text(person));
        }
    }
}