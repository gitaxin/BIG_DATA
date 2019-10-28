package HDFS2HBase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:38 2019/8/27
 */
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {


    /**
     * 文件中数据格式如下
     * 1001	Apple	Red
     * 1002	Pear	Yellow
     * 1003	Pineapple	Yellow
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");


        String rowKey = fields[0];
        String name = fields[1];
        String color = fields[2];


        //初始化rowKey
        ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        Put put = new Put(rowKeyWritable.get());
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"),  Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"),  Bytes.toBytes(color));


       context.write(rowKeyWritable,put);

    }
}
