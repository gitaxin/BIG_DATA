package ETLLOG;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class LoggerMapper extends Mapper<LongWritable,Text,Text,NullWritable> {


    private String djfsf = "fkdjs;fjaks;fdf";
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Test => String
        String line = value.toString();

        //2.解析日志
        //line ：参数数据
        //context:联系上下文，有一个功能：实现计数器的功能
        boolean result = parseLog(line, context);
        //如果数据字段小于等于11的话，就返回
        if(!result){
           return;
        }


        k.set(line);

        //4.context联系上下文
        context.write(k,NullWritable.get());
    }


    /**
     *解析日志
     * 日志长度大于11的话，就计数，并返回true，否则返回false
     * true:计数
     * false：计数
     *
     * @param line
     * @param context
     * @return
     */
    private boolean parseLog(String line, Context context) {
        //1.截取
        String[] fields = line.split(" ");

        if(fields.length > 11){
            context.getCounter("map", "true").increment(1);
            return true;
        }else{
            context.getCounter("map", "false").increment(1);
            return false;
        }

    }
}
