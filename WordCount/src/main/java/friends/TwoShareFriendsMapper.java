package friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:09 2019/8/1
 */
public class TwoShareFriendsMapper extends Mapper<LongWritable,Text,Text,Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");
        String friend = fields[0];
        String[] persons = fields[1].split(",");

        Arrays.sort(persons);

        for (int i = 0; i < persons.length-1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text(persons[i] +"-" + persons[j]),new Text(friend));
            }
        }

    }
}
