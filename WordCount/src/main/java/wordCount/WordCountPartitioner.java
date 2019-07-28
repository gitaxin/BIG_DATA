package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordCountPartitioner extends Partitioner<Text, IntWritable> {


    @Override
    public int getPartition(Text key, IntWritable value, int i) {

        String num = key.toString().substring(0, 1);
        Integer result = Integer.valueOf(num);

        return result % 3;

//        if(result % 3 == 0){
//            return 0;
//        }else{
//            return 1;
//        }

    }
}
