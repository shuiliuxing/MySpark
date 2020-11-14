package org.bing.dataframe.Action操作

import org.apache.spark.sql.SparkSession

object first {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    first1(ss)
  }

  def first1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    println(df.first())
  }

}
