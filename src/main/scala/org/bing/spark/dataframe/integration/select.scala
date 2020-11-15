package org.bing.spark.dataframe.integration

import org.apache.spark.sql.SparkSession

object select {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    select2(ss)
  }

  def select1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.select("name","age").show()
  }

  def select2(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.select(df("name"), df("age")+1).show()
  }
}
