package org.bing.spark.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}
/*
  将RDD转化为数组
 */
object collect {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Collect Test").setMaster("local")
    val sc=new SparkContext(conf)
    collect1(sc)
  }

  def collect1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(1 to 5,4)
    val collectArr=rdd.collect
    for(i <- collectArr)
      println(i)
  }
}
