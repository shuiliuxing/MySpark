package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object union {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Union Test").setMaster("local")
    val sc=new SparkContext(conf)

    union1(sc)
  }

  def union1(sc:SparkContext):Unit={
    val rdd1=sc.makeRDD(List("张三","李四"))
    val rdd2=sc.makeRDD(List("tom","marry"))
    val unionRdd=rdd1.union(rdd2)
    unionRdd.foreach(println)
  }
}
