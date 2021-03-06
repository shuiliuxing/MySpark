package org.bing.spark.dataframe.action

import org.apache.spark.sql.SparkSession

object show {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    import ss.implicits._
    show1(ss)
  }

  def show1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.show()
  }
}
