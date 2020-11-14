package org.bing.dataframe.基础函数

import org.apache.spark.sql.SparkSession
/*
  注册DataFrame为临时表
  registerTempTable--版本1.5.x
  createOrReplaceTempView--版本2.x以上
 */
object createOrReplaceTempView {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("CreateOrReplaceTempView Test").master("local[2]").getOrCreate()
    createOrReplaceTempView1(ss)
  }

  def createOrReplaceTempView1(ss:SparkSession):Unit={
    val df=ss.read.option("header","true").csv("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\movies.csv")
    df.createOrReplaceTempView("t_movie")
    val selectDF=ss.sql("select movieId,title from t_movie where movieId>=5 and movieId<=10")
    selectDF.write.format("csv").save("E:\\data\\spark\\dataframe\\test\\write\\createOrReplaceTempView\\createOrReplaceTempView1")
  }
}
