package com.rr
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.storage.StorageLevel

object SparkTcpStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("TcpStreamReceiver")

    val ssc = new StreamingContext(conf, Seconds(60))
    //val ssc = new StreamingContext(sc, Seconds(1))
   // ssc.checkpoint("/home/notroot/rr/checkpoint/")
    val lines = ssc.socketTextStream("localhost", 8585, StorageLevel.MEMORY_ONLY)
    val words = lines.flatMap(_.split(" "))
    val wordMap = words.map(word => (word, 1))

    //.reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)    

    val wordCounts = wordMap.reduceByKey((first, second) => first + second)
    // wordCounts.print()
      wordCounts.foreachRDD(rdd => if (!rdd.isEmpty()) {
      rdd.collect().foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}