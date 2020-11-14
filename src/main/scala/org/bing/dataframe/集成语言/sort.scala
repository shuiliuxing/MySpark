package org.bing.dataframe.集成语言

import org.apache.spark.sql.SparkSession

object sort {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Sort Test").master("local").getOrCreate()
    sort3(ss)
  }

  def sort1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.sort("age").show()
  }

  def sort2(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    //df.sort("name","age").show()
    df.sort(df("name"),df("age")).show()
  }

  def sort3(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.sort(df("name").desc,df("age").asc).show()
  }
}
