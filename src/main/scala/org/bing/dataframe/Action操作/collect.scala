package org.bing.dataframe.Action操作

import org.apache.spark.sql.SparkSession

object collect {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    collect1(ss)
  }

  def collect1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.collect().foreach(println)
  }
}
