import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object windows {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","work")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SparkFlumeNGWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://master:9000/checkpoint")
    //创建kafka对象   生产者 和消费者
    //模式1 采取的是 receiver 方式  reciver 每次只能读取一条记录
    val topic = Map("mydemo2" -> 1)
    //直接读取的方式  由于kafka 是分布式消息系统需要依赖Zookeeper
  val InputDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "192.168.0.135:2181", "mygroup", topic, StorageLevel.MEMORY_AND_DISK)
    //数据累计计算

    val windowDstream: DStream[(String, String)] = InputDStream.window(Seconds(10),Seconds(5))


    windowDstream.foreachRDD{
      RDD=>RDD.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
    }

//    InputDStream.foreachRDD{
//      RDD =>RDD.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
//    }
//    InputDStream.foreachRDD{
//      RDD => RDD.map((_,1)).collect().foreach(println)  //测试说明 从kafka 取出的数是key-value 的形式的
//    }
//    ((null,sd),1)
//    ((null,asvas),1)
//    ((null,dva),1)


    //    val updateFunc =(curVal:Seq[Int],preVal:Option[Int])=>{
//      //进行数据统计当前值加上之前的值
//      var total = curVal.sum
//      //最初的值应该是0
//      var previous = preVal.getOrElse(0)
//      //Some 代表最终的返回值
//      Some(total+previous)
//    }
//    val result = data.map(_._2).flatMap(_.split(" ")).map(word=>(word,1)).updateStateByKey(updateFunc).print()
     //启动ssc
    ssc.start()
    ssc.awaitTermination()

  }


}
