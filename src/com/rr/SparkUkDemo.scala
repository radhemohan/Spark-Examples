package com.rr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD

object SparkUkDemo {

  case class csvData(city: String, yyyy: String, mm: String, tmax: String, tmin: String, af: String, rain: Float, sun: String)
  //below function i have wrritten to remove starting 7 lines if indx is 0 from RDD
  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(7) //removing starting 7 lines from each file
      }
      lines
    })
  }
  def main(args: Array[String]): Unit = {
    val dir: String = args(0)// "/home/notroot/test_jars/data/"
    val stations: Array[String] = Array("armagh", "ballypatrick", "bradford","braemar","camborne","cambridge")
    val conf = new SparkConf().setMaster("local[3]").setAppName("SparkSqlTest")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    val rdd = sc.textFile(dir + "aberporthdata.txt")
    val withoutHeader: RDD[String] = dropHeader(rdd)
    var rowRDD = withoutHeader.map(_.split(" ").filterNot(_ == "").map(x => if (x.trim().equals("---")) "0" else x).map(x => if (x.contains("#") || x.contains("*")) x.dropRight(1) else x))
      //.map(r => csvData("aberporth",r(0), r(1).toInt, r(2).toFloat, r(3).toFloat, r(4).toFloat, r(5).toFloat, r(6).toFloat))
      .map(r => csvData("aberporth", r(0), r(1), r(2), r(3), r(4), r(5).toFloat, r(6)))
    
    for (stn <- stations) {
      val rdd1 = sc.textFile(dir + stn + "data.txt")
      val withoutHeader1: RDD[String] = dropHeader(rdd1)
      val rowRDD1 = withoutHeader1.map(_.split(" ").filterNot(_ == "").map(x => if (x.trim().equals("---")) "0" else x).map(x => if (x.contains("#") || x.contains("*")) x.dropRight(1) else x))
        .map(r => csvData(stn, r(0), r(1), r(2), r(3), r(4), r(5).toFloat, r(6)))
      rowRDD = sc.union(rowRDD, rowRDD1)
    }
    rowRDD.toDF().registerTempTable("stn_table")

    val rankStnByOnline = hiveContext.sql("SELECT city ,count(*) as count , dense_rank() OVER (ORDER BY count(*) DESC) as rank FROM stn_table group by city")
    val maxRain = hiveContext.sql("SELECT city,yyyy, max(rain) as best_rain, min(sun) as worst_sun, dense_rank() OVER (PARTITION BY yyyy ORDER BY city DESC) as rank FROM stn_table group by city,yyyy order by city LIMIT 200")
    val rankStnByRainFall = hiveContext.sql("SELECT city ,MAX(rain) , dense_rank() OVER (ORDER BY MAX(rain) DESC) as rank FROM stn_table group by city")
    val worseRainByStn = hiveContext.sql("SELECT city ,yyyy,MIN(rain) FROM stn_table group by city,yyyy")
    val worseRainByStn1 = hiveContext.sql("SELECT distinct city,MAX(yyyy) as yyyy,MIN(rain) as rain FROM stn_table group by city")
    val avgForMay = hiveContext.sql("SELECT city,AVG(rain) as avg_rain FROM stn_table where mm='5'  group by city,yyyy order by city,yyyy")
   
    rankStnByOnline.map(row=>"%s, %s,%s".format(row.getString(0),row.getInt(1),row.getInt(2))).coalesce(1,true).saveAsTextFile("file:///home/notroot/rr/byOnline.txt")
    maxRain.map(row=>"%s, %s,%s,%s,%s".format(row.getString(0),row.getString(1),row.getFloat(2),row.getString(3),row.getInt(4))).coalesce(1,true).saveAsTextFile("file:///home/notroot/rr/MaxRain.txt")
    rankStnByRainFall.map(row=>"%s, %s,%s".format(row.getString(0),row.getFloat(1),row.getInt(2))).coalesce(1,true).saveAsTextFile("file:///home/notroot/rr/rankStnByRainFall.txt")
   // avgForMay.map(row=>"%s, %s,%s".format(row.getString(0),row.getFloat(1),row.getLong(2))).coalesce(1,true).saveAsTextFile("file:///home/notroot/rr/rankStnByRainFall.txt")
  
    worseRainByStn1.map(row=>"%s, %s".format(row.getString(0),row.getFloat(1))).coalesce(1,true).saveAsTextFile("file:///home/notroot/rr/worseRain.txt")
   

  }
}