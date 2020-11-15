package org.bing.spark.dataframe.integration

import org.apache.spark.sql.SparkSession

object filter {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Filter Test").master("local").getOrCreate()
    filter2(ss)
  }

  def filter1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    //df.filter(df("age")<25).show()
    df.filter("age<25").show()
  }

  def filter2(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people1.json")
    df.filter("name=='Andy'").show()
  }


}
