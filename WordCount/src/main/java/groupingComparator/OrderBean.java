package groupingComparator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 17:24 2019/7/28
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;

    private double price;


    @Override
    public int compareTo(OrderBean o) {
        //id升序，价格降序
        int result;

        if(this.orderId > o.getOrderId()){
            result = 1;

        }else if(this.orderId < o.getOrderId()){
            result =  -1;
        }else {

            result = o.price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();

    }



    @Override
    public String toString() {
        return orderId + "\t" +  price;

    }
}
