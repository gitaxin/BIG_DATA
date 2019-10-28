package day0719

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Axin in 2019/10/22 23:39  
  */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {


    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    // get input data by connecting to the socket
    val text = env.socketTextStream("192.168.110.111", 9000, '\n')


   var windowCounts = text.flatMap{w => w.split(" ")}
          .map{ w => WordWithCount(w,1)}
          .keyBy("word")
          .timeWindow(Time.seconds(5),Time.seconds(1))
          .sum("count")


    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)


    env.execute("Socket Window WordCount")
  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
