package day0719;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * Created by Axin in 2019/10/22 22:31
 */
public class WordCountBatch {

    public static void main(String[] args) throws Exception {
        //构建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataSet<String> text = env.fromElements("hello flink I Love China I Love Beijing");

        //数据处理
        DataSet<Tuple2<String,Integer>> wordcount = text.flatMap(new LineSplitter())
                .groupBy(0)//0:代表Tuple2 中的key
                .sum(1);//1:代表代表Tuple2中的value

        //sink打印
        //不需要调用execult方法，因为DataSet的api中已经包含了execult操作，print()方法中已经包含了execult
        wordcount.print();


    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] word = s.split(" ");

            for (String str : word) {
                out.collect(new Tuple2<String,Integer>(str,1));

            }

        }
    }
}
