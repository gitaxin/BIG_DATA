package groupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 18:00 2019/7/28
 */
public class OrderPartitioner extends Partitioner<OrderBean,NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        return (orderBean.getOrderId() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
