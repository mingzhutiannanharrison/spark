import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {


    //创建sparkContext
    val configStr = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(configStr)

    //创建streamingContext
    val scc = new StreamingContext(sc, Seconds(5))

    //去掉多余的日志,影响观看
    sc.setLogLevel("WARN")

    //创建receive获取socket数据
    val lines: ReceiverInputDStream[String] = scc.socketTextStream("192.168.0.135", 9999)

    //计数处理,以逗号划分,分成一个个字符串;对每个字符串进行处理成值为1的元组;对相同单词进行相加;进行打印
    val value: DStream[(String, Int)] = lines.flatMap(_.split("\\,")).map((_, 1)).reduceByKey(_ + _)
    value.print()

    //开启并阻塞线程，以保持不断获取
    scc.start()
    scc.awaitTermination()
  }
}