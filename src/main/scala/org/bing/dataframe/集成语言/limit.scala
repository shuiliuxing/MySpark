package org.bing.dataframe.集成语言

import org.apache.spark.sql.SparkSession

object limit {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Filter Test").master("local").getOrCreate()
    limit1(ss)
  }

  def limit1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people1.json")
    df.limit(2).show()
  }
}
