package com.rr
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Minutes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.DataFrame

object SparkStreamingKafka {

  case class csvData(city: String, yyyy: String, mm: String, tmax: String, tmin: String, af: String, rain: Float, sun: String)
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("SparkKafkaStream").set("spark.driver.allowMultipleContexts", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(1))
    //ssc.checkpoint("/home/notroot/rr/checkpoint/")
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("spark-topic1" -> 5))
    //kafkaStream.print()
    val rdd = kafkaStream.map(_._2)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    var rowRDD = rdd.map(_.split(" ").filterNot(_ == "").map(x => if (x.trim().equals("---")) "0" else x))
      .map(r => csvData("aberporth", r(0), r(1), r(2), r(3), r(4), r(5).toFloat, r(6)))
    var df: DataFrame = null
    rowRDD.foreachRDD(r => df = r.toDF());
    df.registerTempTable("stn_table")

    val rankStnByOnline = hiveContext.sql("SELECT city ,count(*) as count , dense_rank() OVER (ORDER BY count(*) DESC) as rank FROM stn_table group by city")
    val maxRain = hiveContext.sql("SELECT city,yyyy, max(rain) as best_rain, min(sun) as worst_sun, dense_rank() OVER (PARTITION BY yyyy ORDER BY city DESC) as rank FROM stn_table group by city,yyyy order by city LIMIT 200")
    val rankStnByRainFall = hiveContext.sql("SELECT city ,MAX(rain) , dense_rank() OVER (ORDER BY MAX(rain) DESC) as rank FROM stn_table group by city")
    val worseRainByStn = hiveContext.sql("SELECT city ,yyyy,MIN(rain) FROM stn_table group by city,yyyy")
    val worseRainByStn1 = hiveContext.sql("SELECT distinct city,MAX(yyyy) as yyyy,MIN(rain) as rain FROM stn_table group by city")
    val avgForMay = hiveContext.sql("SELECT city,AVG(rain) as avg_rain FROM stn_table where mm='5'  group by city,yyyy order by city,yyyy")

    rankStnByOnline.map(row => "%s, %s,%s".format(row.getString(0), row.getInt(1), row.getInt(2))).coalesce(1, true).saveAsTextFile("file:///home/notroot/rr/byOnline.txt")
    maxRain.map(row => "%s, %s,%s,%s,%s".format(row.getString(0), row.getString(1), row.getFloat(2), row.getString(3), row.getInt(4))).coalesce(1, true).saveAsTextFile("file:///home/notroot/rr/MaxRain.txt")
    rankStnByRainFall.map(row => "%s, %s,%s".format(row.getString(0), row.getFloat(1), row.getInt(2))).coalesce(1, true).saveAsTextFile("file:///home/notroot/rr/rankStnByRainFall.txt")
    // avgForMay.map(row=>"%s, %s,%s".format(row.getString(0),row.getFloat(1),row.getLong(2))).coalesce(1,true).saveAsTextFile("file:///home/notroot/rr/rankStnByRainFall.txt")

    worseRainByStn1.map(row => "%s, %s".format(row.getString(0), row.getFloat(1))).coalesce(1, true).saveAsTextFile("file:///home/notroot/rr/worseRain.txt")

  }
}