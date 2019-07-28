package ETLLOG2;

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
        LogBean logBean = parseLog(line, context);
        //如果数据字段小于等于11的话，就返回
        if(!logBean.isValid()){
            return;
        }




        k.set(logBean.toString());

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
    private LogBean parseLog(String line, Context context) {
        //1.截取
        String[] fields = line.split(" ");

        LogBean logBean = new LogBean();

        if(fields.length > 11){
            logBean.setValid(true);
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            //如果字段大于12，就拼接浏览器
            if(fields.length > 12){
                logBean.setHttp_user_agent(fields[11] + " " + fields[12]);
            }else{
                logBean.setHttp_user_agent(fields[11]);
            }

            if(Integer.parseInt(logBean.getStatus()) >= 400){
                logBean.setValid(false);
            }

        }else{
            logBean.setValid(false);
        }

        return logBean;

    }
}
