package day0719;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by Axin in 2019/10/22 22:59
 */
public class WordCountStreaming {

    public static void main(String[] args) throws Exception {
        int port = 9000;

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * [root@bigdata111 ~]# nc -l 9000
         */
        DataStreamSource<String> text = env.socketTextStream("192.168.110.111", port, "\n");

        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> out) throws Exception {

                String[] word = s.split(" ");
                for (String str : word) {

                    out.collect(new WordWithCount(str, 1L));
                }

            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
            //第一个时间是窗口大小，第二个时间每次滑动多长
        //理解 ：监听前几秒的数据

        windowCount.print();
        env.execute("streaming word count");
    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
