package org.bing.dataframe.集成语言

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object agg {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("OrderBy Test").master("local").getOrCreate()
    agg2(ss)
  }

  def agg1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.agg(max("name"),min("age")).show()
  }

  def agg2(ss:SparkSession):Unit={
    val df=ss.read.option("header","true").csv("E:\\data\\spark\\dataframe\\test\\read\\movies1.csv")
//    val aggDf=df.groupBy("genres").agg(max("movieId")).show()
val aggDf=df.groupBy().agg(Map("movieId" -> "max","movieId" -> "min")).show()
//    aggDf.write.format("csv").save("E:\\data\\spark\\dataframe\\test\\write\\agg\\agg2")
//    aggDf.rdd.repartition(1).saveAsTextFile("E:\\data\\spark\\dataframe\\test\\write\\agg\\agg3")
  }
}
