package mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 21:54 2019/7/29
 */

/**
 * 学习点1：两个表join
 * 学习点2 ：小表加缓存
 * 学习点3: Setup
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String,String> map = new HashMap<>();
    //    Ctrl+ Shift + Alt + S 查看语言等级

    Text k = new Text();



    /**
     * 初始化方法
     * 加载pd.txt
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override

    protected void setup(Context context) throws IOException, InterruptedException {

        //把文件加载进来
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("E:\\test_input\\pd.txt")), "utf-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\t");
            String pid = fields[0];
            String pname = fields[1];
            map.put(pid, pname);
        }
        reader.close();

    }

    /**
     * order.txt
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String orderId = fields[0];
        String pid = fields[1];
        String amount = fields[2];

        String pname = map.get(pid);


        k.set(orderId + "\t" + pname + "\t" + amount);
        context.write(k, NullWritable.get());

    }
}
