package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object filter {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Filter Test").setMaster("local")
    val sc=new SparkContext(conf)
    filter2(sc)
  }

  //简单1
  def filter1(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\movies.csv")
    val filterRdd=rdd.filter(line=>line.contains("Drama"))
    filterRdd.foreach(println)
  }

  //简单2
  def filter2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(1,2,3,7,4,5,8))
    val filterRdd=rdd.filter(x=>x>=4)
    filterRdd.foreach(println)
  }
}
