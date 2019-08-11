package reduceJson;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 21:18 2019/7/30
 */
public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();

        String line = value.toString();
        String[] fields = line.split("\t");
        if (name.startsWith("order")) { //按订单表处理
            tableBean.setOrderId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPName("");
            tableBean.setFlag("0");

            k.set(fields[1]);
        } else {
            //按产品表处理
            tableBean.setPid(fields[0]);
            tableBean.setPName(fields[1]);
            tableBean.setAmount(0);
            tableBean.setOrderId("");
            tableBean.setFlag("1");

            k.set(fields[0]);

        }

        context.write(k, tableBean);

    }
}
