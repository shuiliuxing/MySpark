package org.bing.spark.dataframe.basicfunc

import org.apache.spark.sql.SparkSession
import org.bing.spark.dataframe.basicfunc.toDF.toDF2

object columns {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    columns1(ss)
  }

  def columns1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.columns.foreach(println)
  }

}
