package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}
/*
  并行整合RDD中所有元素
 */
object reduce {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Reduce Test").setMaster("local")
    val sc=new SparkContext(conf)
    reduce2(sc)
  }

  def reduce1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(List("a","ab","abc","abcd","abcde"))
    val reduceNum=rdd.map(x=>x.length).reduce((x,y)=>x+y)
    println(reduceNum)
  }

  def reduce2(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\app1.log")
    val reduceNum=rdd.map(line=>line.split(" ").size).reduce((a,b)=> if(a>b) a else b)
    println(reduceNum)
  }
}
