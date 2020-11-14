package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object intersection {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Intersection Test").setMaster("local")
    val sc=new SparkContext(conf)
    intersection1(sc)
  }

  def intersection1(sc:SparkContext):Unit={
    val rdd1=sc.makeRDD(List("张三","李四","王五","tom"))
    val rdd2=sc.makeRDD(List("tom","lilei"))
    val intersectionRdd=rdd1.intersection(rdd2)
    intersectionRdd.foreach(println)
  }
}
