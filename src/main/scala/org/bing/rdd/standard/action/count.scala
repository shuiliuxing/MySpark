package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}

object count {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Count Test").setMaster("local")
    val sc=new SparkContext(conf)

    count2(sc)
  }

  def count1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(1 to 5)
    println(rdd.count())
  }

  def count2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(("A","1"),("B",2),("C",3)))
    val countNum=rdd.count
    println(countNum)
  }
}
