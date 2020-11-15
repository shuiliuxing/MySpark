package org.bing.spark.dataframe.action

import org.apache.spark.sql.SparkSession

object count {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    import ss.implicits._
    count1(ss);
  }

  def count1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    println(df.count)
  }
}
