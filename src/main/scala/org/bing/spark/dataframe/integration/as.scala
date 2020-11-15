package org.bing.spark.dataframe.integration

import org.apache.spark.sql.SparkSession
/*
  以一个别名集的方式返回一个新DataFrame
 */
object as {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("AS Test").master("local").getOrCreate()
    val rdd=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    rdd.show()
  }
}
