package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object distinct {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Distinct Test").setMaster("local")
    val sc=new SparkContext(conf)

    distinct1(sc)
  }

  //简单1

  def distinct1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List("张三","李四","王五","李四","王五"))
    val distinctRdd=rdd.distinct()
    distinctRdd.foreach(println)
  }
}
