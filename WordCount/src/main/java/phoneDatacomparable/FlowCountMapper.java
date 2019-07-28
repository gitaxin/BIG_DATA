package phoneDatacomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * LongWritable, Text ===> Map输入
 * Text,FlowBean ===> Map的输出：手机号、流量上传下载总和
 * @Author: Axin
 * @Date: Create in 14:21 2019/7/28
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    //FlowBean k = new FlowBean();
    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取每一行数据
        String line = value.toString();
        String[] fields = line.split("\t");
        //手机号
        String phone = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        v.set(phone);
        context.write(new FlowBean(upFlow,downFlow),v);
    }
}
