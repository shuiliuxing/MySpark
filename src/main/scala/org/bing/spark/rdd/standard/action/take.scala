package org.bing.spark.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}
/*
  返回前n个元素
 */
object take {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Count Test").setMaster("local")
    val sc=new SparkContext(conf)
    
    take2(sc)
  }

  def take1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(4,8,3,0,1,9))
    val takeRdd=rdd.take(2)
    takeRdd.foreach(println)
  }

  def take2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array("a","ab","abc","abcd"))
    val takeRdd=rdd.map(line=>(line,line.length)).take(3)
    takeRdd.foreach(println)
  }
}
