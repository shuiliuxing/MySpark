package org.bing.dataframe.基础函数

import org.apache.spark.sql.SparkSession

object schema {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    schema1(ss)
  }

  def schema1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    println(df.schema)
  }
}
