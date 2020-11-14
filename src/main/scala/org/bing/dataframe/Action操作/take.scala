package org.bing.dataframe.Action操作

import org.apache.spark.sql.SparkSession

object take {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Take Test").master("local[2]").getOrCreate()
    take1(ss)
  }

  def take1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.take(2).foreach(println)
  }
}
