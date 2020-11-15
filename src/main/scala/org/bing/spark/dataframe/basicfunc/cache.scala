package org.bing.spark.dataframe.basicfunc

import org.apache.spark.sql.SparkSession

object cache {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    cache1(ss)
  }

  def cache1(ss:SparkSession):Unit={
    val df=ss.read.option("header","true").csv("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores.csv")
    df.cache()

    val start1=System.currentTimeMillis()
    val rows1=df.count()
    val end1=System.currentTimeMillis()
    println("行数："+rows1+" , "+(end1-start1)+"毫秒")
    val start2=System.currentTimeMillis()
    val rows2=df.count()
    val end2=System.currentTimeMillis()
    println("行数："+rows2+" , "+(end2-start2)+"毫秒")
  }

}
