package org.bing.dataframe.集成语言

import org.apache.spark.sql.SparkSession

object orderBy {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("OrderBy Test").master("local").getOrCreate()
    orderBy1(ss)
  }

  def orderBy1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.orderBy(df("name"),df("age").asc).show()
  }
}
