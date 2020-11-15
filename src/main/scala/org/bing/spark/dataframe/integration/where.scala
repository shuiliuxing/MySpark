package org.bing.spark.dataframe.integration

import org.apache.spark.sql.SparkSession

object where {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    where2(ss)
  }

  def where1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.where("age<20").show()
  }

  def where2(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.where("name=='Justin'").show()
  }
}
