package org.bing.dataframe.集成语言

import org.apache.spark.sql.SparkSession

object groupBy {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Filter Test").master("local").getOrCreate()
    groupBy3(ss)
  }

  def groupBy1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people1.json")
    df.groupBy("name").count().show()
  }

  def groupBy2(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people1.json")
    df.groupBy("name","age").count().show()
  }

  def groupBy3(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people1.json")
    df.createOrReplaceTempView("people")  //注册临时表
    val groupByDf=ss.sql("select name,count(name) as num from people group by name")
    groupByDf.show()
  }

}
