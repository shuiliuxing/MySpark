package org.bing.spark.dataframe.output

import org.apache.spark.sql.SparkSession

object save {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("SaveAsTextFile").master("local").getOrCreate()
    save1(ss)
  }

  def save1(ss:SparkSession):Unit={
    val df=ss.read.json("E:\\data\\spark\\dataframe\\test\\read\\people.json")
    df.write.format("csv").save("E:\\data\\spark\\dataframe\\test\\write\\save\\save1")
  }

  def save2(ss:SparkSession):Unit={
    val df=ss.read.option("header","true").csv("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\movies.csv")
    df.createOrReplaceTempView("t_movie")
    val selectDF=ss.sql("select movieId,title from t_movie where movieId>=5 and movieId<=10")
    selectDF.write.format("csv").save("E:\\data\\spark\\dataframe\\test\\write\\save\\save2")
  }

}
