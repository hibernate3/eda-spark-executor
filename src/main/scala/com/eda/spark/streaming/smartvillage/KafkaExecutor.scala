package com.eda.spark.streaming.smartvillage

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import kafka.serializer.StringDecoder

object KafkaExecutor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://bigdata-dev-41:8020/temp/test/SparkStreaming")

    val topics = Set("community_car")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "bigdata-kafka-01:9092, bigdata-kafka-02:9092, bigdata-kafka-03:9092"
    )
    val data = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val result = data.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
