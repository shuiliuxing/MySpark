package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}
/*
  降序
 */
object top {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Top Test").setMaster("local")
    val sc=new SparkContext(conf)
    top1(sc)
  }

  def top1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(Seq(10,4,2,12,3,1))
    val topRdd=rdd.top(2)
    topRdd.foreach(println)
  }
}
