package org.bing.spark.dataframe.action

import org.apache.spark.sql.SparkSession
/*
  返回第n行数据
 */
object head {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    import ss.implicits._
    head1(ss)
  }

  def head1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    println(df.head())
  }
}
