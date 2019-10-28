package HDFS2HBase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;


/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:46 2019/8/27
 */
public class WriteFruitMRFromTxtReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {


    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
