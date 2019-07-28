package Phone;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 22:16 2019/7/27
 */
public class PhonePartitioner extends Partitioner<Text,NullWritable> {


    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        String phone = text.toString();
        String sub = phone.substring(0, 3);

        if("136".equals(sub)){
            return 0;
        } else if ("137".equals(sub)) {
            return 1;
        } else if ("138".equals(sub)) {
            return 2;
        } else if ("139".equals(sub)) {
            return 3;
        } else if ("135".equals(sub)) {
            return 4;
        } else {
            return 5;
        }



    }


}
