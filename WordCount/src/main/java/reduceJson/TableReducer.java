package reduceJson;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 21:34 2019/7/30
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {


        List<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean bean : values) {
            String flag = bean.getFlag();
            //订单表
            if("0".equals(flag)){
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean,bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            }else{//1:产品表
                try {
                    BeanUtils.copyProperties(pdBean,bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }

        for (TableBean bean : orderBeans) {
            bean.setPName(pdBean.getPName());
            context.write(bean,NullWritable.get());
        }
        


    }
}
