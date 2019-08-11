package reduceJson;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 20:55 2019/7/30
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class TableBean implements Writable {


    private String orderId;//订单id
    private String pid;//产品id
    private int amount;//数量
    private String pName;//产品名称
    private String flag;//标记





    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        pName = in.readUTF();
        flag = in.readUTF();

    }

    @Override
    public String toString() {
        return orderId + "\t" + amount + "\t" + pName + "\t";
    }
}
