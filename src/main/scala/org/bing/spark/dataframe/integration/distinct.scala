package org.bing.spark.dataframe.integration

import org.apache.spark.sql.SparkSession
import org.bing.spark.dataframe.integration.filter.filter1

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
