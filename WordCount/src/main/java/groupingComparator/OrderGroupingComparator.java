package groupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 17:47 2019/7/28
 */
public class OrderGroupingComparator extends WritableComparator {


    public OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean)a;
        OrderBean o2 = (OrderBean)b;

        int result;

        if(o1.getOrderId() > o2.getOrderId()){
            result = 1;
        } else if (o1.getOrderId() < o2.getOrderId()) {
            result = -1;
        } else {
            result = o1.getPrice() > o2.getPrice() ? -1 : 1;
        }

        return result;

    }
}
