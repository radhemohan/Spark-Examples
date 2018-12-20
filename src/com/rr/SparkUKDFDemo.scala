package com.rr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window

object SparkUKDFDemo {

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
    val dir: String = args(0) //"/home/notroot/test_jars/data/"
    val stations: Array[String] = Array("armagh", "ballypatrick", "bradford","braemar","camborne","cambridge")
    val conf = new SparkConf().setMaster("local[3]").setAppName("SparkDF")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val rdd = sc.textFile(dir + "aberporthdata.txt") 
    val withoutHeader: RDD[String] = dropHeader(rdd)
    var rowRDD = withoutHeader.map(_.toString().split(" ").filterNot(_ == "").map(x => if (x.trim().equals("---")) "0" else x).map(x => if (x.contains("#") || x.contains("*")) x.dropRight(1) else x))
      //.map(r => csvData("aberporth", r(0), r(1), r(2), r(3), r(4), r(5).toFloat, r(6))).toDF()
      .map(r => csvData("aberporth", r(0), r(1), r(2), r(3), r(4), r(5).toFloat, r(6)))

    for (stn <- stations) {
      val rdd1 = sc.textFile(dir + stn + "data.txt")
      val withoutHeader1: RDD[String] = dropHeader(rdd1)
      val rowRDD1 = withoutHeader1.map(_.toString().split(" ").filterNot(_ == "").map(x => if (x.trim().equals("---")) "0" else x).map(x => if (x.contains("#") || x.contains("*")) x.dropRight(1) else x))
        .map(r => csvData(stn, r(0), r(1), r(2), r(3), r(4), r(5).toFloat, r(6)))
      rowRDD = sc.union(rowRDD, rowRDD1)

    }
    val df = rowRDD.toDF()
    val wSpec = Window.partitionBy("city").orderBy("city")
    val rankStnByOnline = df.withColumn("count", count("rain").over(wSpec))
    //val cityCount = df.groupBy("city").count()
     rankStnByOnline.show()
    val worseRainByStn = df.groupBy("city").min("rain").distinct()
    worseRainByStn.show()

    val avgForMay = df.filter($"mm" === "5").groupBy("city", "yyyy").avg("rain")
    avgForMay.show()

  }
}