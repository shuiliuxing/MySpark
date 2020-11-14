package org.bing.dataframe.集成语言

import org.apache.spark.sql.SparkSession
import org.bing.dataframe.集成语言.filter.filter1

object distinct {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("Filter Test").master("local").getOrCreate()
    distinct1(ss)
  }

  def distinct1(ss: SparkSession): Unit = {
    val df = ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people1.json")
    df.distinct().show()
  }
}
