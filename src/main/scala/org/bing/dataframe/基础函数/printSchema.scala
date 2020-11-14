package org.bing.dataframe.基础函数

import org.apache.spark.sql.SparkSession

object printSchema {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    printSchema1(ss)
  }

  def printSchema1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.printSchema()
  }
}
